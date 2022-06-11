#!/usr/bin/env python3.8

from telegram.ext.updater import Updater
from telegram.update import Update
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext.messagehandler import MessageHandler
from telegram.ext.filters import Filters
from typing import Tuple, Optional
import os, html, json, traceback, logging
import threading

from telegram import Update, Chat, ChatMember, ParseMode, ChatMemberUpdated
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackContext,
    ChatMemberHandler,
)

from PIL import Image
import imagehash
import imghdr
from rocksdict import Rdict, Options, SliceTransform

TOKEN = ""
CHAT_SUPERGROUP_STR = "-100"
DEVELOPER_CHAT_ID = "-1001789876771"
DEVELOPER_CHAT_USABLE = DEVELOPER_CHAT_ID[4:]
TEMP_DIR = 'tmpdir/'
SIMILARITY_COEF = 5

current_dir = os.getcwd()
TEMP_DIR_FULL_PATH = f"{current_dir}/{TEMP_DIR}"
if not os.path.exists(TEMP_DIR_FULL_PATH):
    os.mkdir(TEMP_DIR_FULL_PATH)


rocksdb_path = f'/{current_dir}/rocksdb'
opt = Options()
# opt.create_if_missing(True)
# opt.create_missing_column_families(True)
opt.set_max_background_jobs(os.cpu_count())
opt.set_write_buffer_size(0x10000000)
opt.set_level_zero_file_num_compaction_trigger(4)
opt.set_max_bytes_for_level_base(0x40000000)
opt.set_target_file_size_base(0x10000000)
opt.set_max_bytes_for_level_multiplier(4.0)
opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
# opt.set_plain_table_factory(PlainTableFactoryOptions())

def init_rocksdb(rocksdb_path, opt) -> Rdict:
    if os.path.exists(f'{rocksdb_path}/CURRENT'):
        cfs = Rdict.list_cf(rocksdb_path)
    else:
        print('No cf has been found. Creating')
        cfs = []
    if not cfs:
        db = Rdict(path=rocksdb_path, options=opt)
        db.create_column_family(DEVELOPER_CHAT_USABLE, opt)
        return db
    cfs_conf = {}
    for cf in cfs:
        cfs_conf[cf] = opt
    db = Rdict(path=rocksdb_path, options=opt, column_families=cfs_conf)
    
    print(f'DB initialized with cf: {cfs}')
    return db

db = init_rocksdb(rocksdb_path=rocksdb_path, opt=opt)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


def extract_status_change(
    chat_member_update: ChatMemberUpdated,
) -> Optional[Tuple[bool, bool]]:
    """Takes a ChatMemberUpdated instance and extracts whether the 'old_chat_member' was a member
    of the chat and whether the 'new_chat_member' is a member of the chat. Returns None, if
    the status didn't change.
    """
    status_change = chat_member_update.difference().get("status")
    old_is_member, new_is_member = chat_member_update.difference().get("is_member", (None, None))

    if status_change is None:
        return None

    old_status, new_status = status_change
    was_member = old_status in [
        ChatMember.MEMBER,
        ChatMember.CREATOR,
        ChatMember.ADMINISTRATOR,
    ] or (old_status == ChatMember.RESTRICTED and old_is_member is True)
    is_member = new_status in [
        ChatMember.MEMBER,
        ChatMember.CREATOR,
        ChatMember.ADMINISTRATOR,
    ] or (new_status == ChatMember.RESTRICTED and new_is_member is True)

    return was_member, is_member


def track_chats(update: Update, context: CallbackContext) -> None:
    """Tracks the chats the bot is in."""
    result = extract_status_change(update.my_chat_member)
    if result is None:
        return
    was_member, is_member = result

    # Let's check who is responsible for the change
    cause_name = update.effective_user.full_name

    # Handle chat types differently:
    chat = update.effective_chat
    if chat.type == Chat.PRIVATE:
        if not was_member and is_member:
            logger.info("%s started the bot", cause_name)
            context.bot_data.setdefault("user_ids", set()).add(chat.id)
        elif was_member and not is_member:
            logger.info("%s blocked the bot", cause_name)
            context.bot_data.setdefault("user_ids", set()).discard(chat.id)
    elif chat.type in [Chat.GROUP, Chat.SUPERGROUP]:
        if not was_member and is_member:
            logger.info("%s added the bot to the group %s. %s", cause_name, chat.title, chat)
            context.bot_data.setdefault("group_ids", set()).add(chat.id)
            # threading.Thread(target=create_column_family(chat.id)).start()
        elif was_member and not is_member:
            logger.info("%s removed the bot from the group %s. %s", cause_name, chat.title, chat)
            context.bot_data.setdefault("group_ids", set()).discard(chat.id)
    else:
        if not was_member and is_member:
            logger.info("%s added the bot to the channel %s", cause_name, chat.title)
            context.bot_data.setdefault("channel_ids", set()).add(chat.id)
        elif was_member and not is_member:
            logger.info("%s removed the bot from the channel %s", cause_name, chat.title)
            context.bot_data.setdefault("channel_ids", set()).discard(chat.id)
    
def shutdown():
    global updater
    updater.stop()
    updater.is_idle = False
    db.close()
    
def error_handler(update: object, context: CallbackContext) -> None:
    """Log the error and send a telegram message to notify the developer."""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    if context.error:
        tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
        tb_string = ''.join(tb_list)
    else:
        tb_string = 'zaloopa'

    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (
        f'An exception was raised while handling an update\n'
        f'<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}'
        '</pre>\n\n'
        f'<pre>context.chat_data = {html.escape(str(context.chat_data))}</pre>\n\n'
        f'<pre>context.user_data = {html.escape(str(context.user_data))}</pre>\n\n'
        f'<pre>{html.escape(tb_string)}</pre>'
    )

    # Finally, send the message
    context.bot.send_message(chat_id=DEVELOPER_CHAT_ID, text=message, parse_mode=ParseMode.HTML)
    
    threading.Thread(target=shutdown).start()
    
def show_chats(update: Update, context: CallbackContext) -> None:
    """Shows which chats the bot is in"""
    user_ids = ", ".join(str(uid) for uid in context.bot_data.setdefault("user_ids", set()))
    group_ids = ", ".join(str(gid) for gid in context.bot_data.setdefault("group_ids", set()))
    channel_ids = ", ".join(str(cid) for cid in context.bot_data.setdefault("channel_ids", set()))
    text = (
        f"@{context.bot.username} is currently in a conversation with the user IDs {user_ids}."
        f" Moreover it is a member of the groups with IDs {group_ids} "
        f"and administrator in the channels with IDs {channel_ids}."
    )
    update.effective_message.reply_text(text)


def greet_chat_members(update: Update, context: CallbackContext) -> None:
    """Greets new users in chats and announces when someone leaves"""
    result = extract_status_change(update.chat_member)
    if result is None:
        return

    was_member, is_member = result
    cause_name = update.chat_member.from_user.mention_html()
    member_name = update.chat_member.new_chat_member.user.mention_html()

    if not was_member and is_member:
        update.effective_chat.send_message(
            f"{member_name} was added by {cause_name}. Welcome!",
            parse_mode=ParseMode.HTML,
        )
    elif was_member and not is_member:
        update.effective_chat.send_message(
            f"{member_name} is no longer with us. Thanks a lot, {cause_name} ...",
            parse_mode=ParseMode.HTML,
        )

def check_hamming_diff(cf, s_hash):
    similarities = []
    for k, v in cf.items():
        diff = imagehash.hex_to_hash(k) - imagehash.hex_to_hash(s_hash)
        if diff <= SIMILARITY_COEF and diff > 0:
            similarities.append(str(v))
    return similarities


def image_handler(update, context):
    if update.message:
        file = update.message.photo[0].file_id
        obj = context.bot.get_file(file)
        obj.download(f"{TEMP_DIR_FULL_PATH}{file}")
        image_type = imghdr.what(f"{TEMP_DIR_FULL_PATH}{file}")
        os.rename(f"{TEMP_DIR_FULL_PATH}{file}", f"{TEMP_DIR_FULL_PATH}{file}.{image_type}")
        img_file = f"{TEMP_DIR_FULL_PATH}{file}.{image_type}"

        # duplicates checking starts here
        hash = imagehash.average_hash(Image.open(img_file), hash_size=16)
        
        chat_id = str(update.message.chat.id)

        if CHAT_SUPERGROUP_STR in chat_id:
            chat_id = chat_id[4:]
        s_hash = str(hash)
        try:
            cf = db.get_column_family(chat_id)
        except Exception as e:
            print(e)
            cf = db.create_column_family(chat_id, opt)
            
        reply_data = ""
        if not s_hash in cf:
            cf[s_hash] = update.message.message_id
        else:
            lookup_data = cf[s_hash]
            reply_data += f"Looks like it's repost of https://t.me/c/{chat_id}/{lookup_data}\n"
        
        similar_images = check_hamming_diff(cf, s_hash)
        if similar_images:
            for s in similar_images:
                reply_data += f"Similar to https://t.me/c/{chat_id}/{s}\n"
            
        if reply_data:
            update.message.reply_text(reply_data)
        cf.close()
        
        os.remove(img_file)


def serve():
    print("Bot started")
    global updater 
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.MY_CHAT_MEMBER))
    dispatcher.add_handler(CommandHandler("show_chats", show_chats))
    dispatcher.add_handler(CommandHandler("generate_error", error_handler))
    dispatcher.add_handler(ChatMemberHandler(greet_chat_members, ChatMemberHandler.CHAT_MEMBER))
    dispatcher.add_handler(MessageHandler(Filters.photo, image_handler))
    # dispatcher.add_handler(CommandHandler('yesterday', yesterday, pass_args=True))
    dispatcher.add_error_handler(error_handler)
    updater.start_polling(allowed_updates=Update.ALL_TYPES)
    updater.idle()

def main() -> None:
    while True:
        serve()
    
if __name__ == "__main__":
    main()
    
