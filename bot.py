#!/usr/bin/env python3.8

from email import message
from telegram.ext.updater import Updater
from telegram.update import Update
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext.messagehandler import MessageHandler
from telegram.ext.filters import Filters

import logging
from typing import Tuple, Optional

from telegram import Update, Chat, ChatMember, ParseMode, ChatMemberUpdated
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackContext,
    ChatMemberHandler,
)

from PIL import Image
import imagehash
import os
import imghdr
from bloom_filter2 import BloomFilter
import diskhash

TOKEN = ""
TEMP_DIR = 'tmpdir/'

current_dir = os.getcwd()
TEMP_DIR_FULL_PATH = f"{current_dir}/{TEMP_DIR}"
if not os.path.exists(TEMP_DIR_FULL_PATH):
    os.mkdir(TEMP_DIR_FULL_PATH)
  
bloom = BloomFilter(max_elements=1000000, error_rate=0.01, filename=f"{current_dir}/bloom.bin")
tb = diskhash.Str2int(f"{current_dir}/hash_msg.dht", 17, 'rw') #key len, struct format

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
            logger.info("%s added the bot to the group %s", cause_name, chat.title)
            context.bot_data.setdefault("group_ids", set()).add(chat.id)
        elif was_member and not is_member:
            logger.info("%s removed the bot from the group %s", cause_name, chat.title)
            context.bot_data.setdefault("group_ids", set()).discard(chat.id)
    else:
        if not was_member and is_member:
            logger.info("%s added the bot to the channel %s", cause_name, chat.title)
            context.bot_data.setdefault("channel_ids", set()).add(chat.id)
        elif was_member and not is_member:
            logger.info("%s removed the bot from the channel %s", cause_name, chat.title)
            context.bot_data.setdefault("channel_ids", set()).discard(chat.id)


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
        
def alpharemover(image):
    if image.mode != 'RGBA':
        return image
    canvas = Image.new('RGBA', image.size, (255,255,255,255))
    canvas.paste(image, mask=image)
    return canvas.convert('RGB')

def image_handler(update, context):
    # temp_name = next(tempfile._get_candidate_names())
    file = update.message.photo[0].file_id
    
    print(update.message)
    print(update.message.chat.id, update.message.message_id)
    obj = context.bot.get_file(file)
    obj.download(f"{TEMP_DIR_FULL_PATH}{file}")
    image_type = imghdr.what(f"{TEMP_DIR_FULL_PATH}{file}")
    os.rename(f"{TEMP_DIR_FULL_PATH}{file}", f"{TEMP_DIR_FULL_PATH}{file}.{image_type}")
    img_file = f"{TEMP_DIR_FULL_PATH}{file}.{image_type}"



    image = alpharemover(Image.open(img_file))
    image = image.convert("L").resize((100, 100), Image.ANTIALIAS)
    hash = imagehash.average_hash(image)
    
    is_in_bloom = 0
    if str(hash) in bloom:
        is_in_bloom = 1
    
    # hashobj = imagehash.hex_to_hash('f7e3c1c3e3e1c181')
    print(hash)
    
    for r in range(0,360,90):
        rothash = imagehash.average_hash(image.rotate(r))
        if str(rothash) in bloom:
            is_in_bloom = 1
        # print(f'Rotation by {r}: {hashobj - rothash} Hamming difference (rothash {rothash})')
    
    
    if not is_in_bloom:
        bloom.add(str(hash))
        tb.insert(str(hash), int(update.message.message_id))
        print(tb.lookup(str(hash)))
    else:
        lookup_data = tb.lookup(str(hash))
        tg_link = f"https://t.me/c/{str(update.message.chat.id)[4:]}/{lookup_data}"
        update.message.reply_text(f"{tg_link}")
        
    os.remove(img_file)


def main() -> None:
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.MY_CHAT_MEMBER))
    dispatcher.add_handler(CommandHandler("show_chats", show_chats))
    dispatcher.add_handler(ChatMemberHandler(greet_chat_members, ChatMemberHandler.CHAT_MEMBER))
    dispatcher.add_handler(MessageHandler(Filters.photo, image_handler))
    updater.start_polling(allowed_updates=Update.ALL_TYPES)
    updater.idle()
    
if __name__ == "__main__":
    main()
    
