# imports 
#!/usr/bin/env python3.11

import html
import json
import urllib 
import logging
import traceback
import os
from typing import Optional, Tuple, Literal, TypeAlias
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

# tg imports
from telegram import __version__ as TG_VER

try:
    from telegram import __version_info__
except ImportError:
    __version_info__ = (0, 0, 0, 0, 0)

if __version_info__ < (20, 0, 0, "alpha", 1):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )
from telegram import Update, ChatMemberUpdated, KeyboardButton, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, ChatMemberHandler, CallbackContext, Updater
from telegram.ext import MessageHandler
from telegram.ext import filters
# end tg imports

from PIL import Image
import imagehash
# from rocksdict import Rdict, Options, SliceTransform
import secrets
import mysql.connector

# end imports 

# globals
TOKEN = ""
OPENWEATHER_APP_ID = ''
DEVELOPER_CHAT_ID = "-1001789876771"
TEMP_DIR = 'tmpdir/'
SIMILARITY_COEF = 4

conn = mysql.connector.connect(
        host = '127.0.0.1',
        port = 3306,
        user = 'root',
        password = '',
        database = 'imagebot',
        buffered = False)
cur = conn.cursor()
# end globals


current_dir = os.getcwd()
TEMP_DIR_FULL_PATH = f"{current_dir}/{TEMP_DIR}"
if not os.path.exists(TEMP_DIR_FULL_PATH):
    os.mkdir(TEMP_DIR_FULL_PATH)

table_structure = ''' (
  `message_id` int(11) NOT NULL,
  `A0` bigint(20) DEFAULT NULL,
  `A1` bigint(20) DEFAULT NULL,
  `A2` bigint(20) DEFAULT NULL,
  `A3` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci'''

# not fastest but will do
hamming_func = '''DELIMITER $$
create function hamming_32(A0 BIGINT, A1 BIGINT, A2 BIGINT, A3 BIGINT, B0 BIGINT, B1 BIGINT, B2 BIGINT, B3 BIGINT) RETURNS BIGINT 
BEGIN 
  DECLARE RET BIGINT; 
  SET ret = BIT_COUNT(A0 ^ B0) + BIT_COUNT(A1 ^ B1) + BIT_COUNT(A2 ^ B2) + BIT_COUNT(A3 ^ B3); 
  return ret;
END;
$$'''

table_prefix= 't'

def tg_to_sql_chat_name(chat:str) -> str:
    return f"{table_prefix}{chat.replace('-','')}"

def mysql_init_table(table_name:str):
    cur.execute("SHOW TABLES;")
    tables = cur.fetchall()
    if tables:
        for t in tables:
            if table_name in t:
                logger.info("existing tables: {}".format(table_name))
            else:
                try:
                    cur.execute(f"CREATE TABLE `{table_name}` {table_structure}")
                except Exception as e:
                    logger.error(e)

def mysql_check_similarity(table, h, i=None):
    table = tg_to_sql_chat_name(table)
    cur.execute(f"SHOW TABLES LIKE '{table}';")
    res = cur.fetchall()
    if not res:
        mysql_init_table(table_name = table)
    s_statement = f"SELECT `message_id` FROM `{table}` WHERE hamming_32(A0, A1, A2, A3,  CONV(SUBSTRING('{h}', 1,  8), 16, 10), CONV(SUBSTRING('{h}', 9,  8), 16, 10),  CONV(SUBSTRING('{h}', 17,  8), 16, 10), CONV(SUBSTRING('{h}', 25,  8), 16, 10)) <= {SIMILARITY_COEF};"
    cur.execute(s_statement)
    res = cur.fetchall()
    if res:
        return (r[0] for r in res)
    else:
        h_arr = [int(h[:8], 16), int(h[8:16], 16), int(h[16:24], 16), int(h[24:32], 16)]
        ins = f'INSERT INTO `{table}` (message_id, A0, A1, A2, A3) VALUES (%s, %s, %s, %s, %s)' % (i, h_arr[0], h_arr[1], h_arr[2], h_arr[3])
        cur.execute(ins)
        conn.commit()
        return None

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def show_chats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows which chats the bot is in"""
    user_ids = ", ".join(str(uid) for uid in context.bot_data.setdefault("user_ids", set()))
    group_ids = ", ".join(str(gid) for gid in context.bot_data.setdefault("group_ids", set()))
    channel_ids = ", ".join(str(cid) for cid in context.bot_data.setdefault("channel_ids", set()))
    text = (
        f"@{context.bot.username} is currently in a conversation with the user IDs {user_ids}."
        f" Moreover it is a member of the groups with IDs {group_ids} "
        f"and administrator in the channels with IDs {channel_ids}."
    )
    await update.effective_message.reply_text(text)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a telegram message to notify the developer."""
    # Log the error before we do anything else, so we can see it even if something breaks.
    logger.error("Exception while handling an update:", exc_info=context.error)

    # traceback.format_exception returns the usual python message about an exception, but as a
    # list of strings rather than a single string, so we have to join them together.
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)

    # Build the message with some markup and additional information about what happened.
    # You might need to add some logic to deal with messages longer than the 4096 character limit.
    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (
        f"An exception was raised while handling an update\n"
        f"<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}"
        "</pre>\n\n"
        f"<pre>context.chat_data = {html.escape(str(context.chat_data))}</pre>\n\n"
        f"<pre>context.user_data = {html.escape(str(context.user_data))}</pre>\n\n"
        f"<pre>{html.escape(tb_string)}</pre>"
    )

    # Finally, send the message
    await context.bot.send_message(
        chat_id=DEVELOPER_CHAT_ID, text=message, parse_mode=ParseMode.HTML
    )


async def bad_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Raise an error to trigger the error handler."""
    await context.bot.wrong_method_name()  # type: ignore[attr-defined]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays info on how to trigger an error."""
    await update.effective_message.reply_html(
        "Use /bad_command to cause an error.\n"
        f"Your chat id is <code>{update.effective_chat.id}</code>."
    )


def extract_status_change(chat_member_update: ChatMemberUpdated) -> Optional[Tuple[bool, bool]]:
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
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (old_status == ChatMember.RESTRICTED and old_is_member is True)

    is_member = new_status in [
        ChatMember.MEMBER,
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (new_status == ChatMember.RESTRICTED and new_is_member is True)

    return was_member, is_member

async def track_chats(update: Update, context: CallbackContext) -> None:
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


async def image_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_file = await context.bot.get_file(update.message.effective_attachment[0].file_id) if update.message else None
    # will remove error message on some images reaction 
    if not new_file:
        return

    # File(file_id='AgACAgIAAx0Caq9aIwACAvlkz9Z85caAHYJGLXBrLZchuEXSOAACaNIxG7SweEpvM4Nl4ntQAAEBAAMCAANzAAMvBA', file_path='https://api.telegram.org/file/bot5483007201:AAG12Nj25PaWb1DVIhdw_2m64tDt2fowR0g/photos/file_9566.jpg', file_size=622, file_unique_id='AQADaNIxG7SweEp4')
    new_file.file_unique_id
    await new_file.download_to_drive(f"{TEMP_DIR_FULL_PATH}{new_file.file_unique_id}")
    # await update.message.reply_text(f"waiting for the first awailable CIPSO agent to check {new_file.file_unique_id}")

    img = Image.open(f"{TEMP_DIR_FULL_PATH}{new_file.file_unique_id}")
    os.rename(f"{TEMP_DIR_FULL_PATH}{new_file.file_unique_id}", f"{TEMP_DIR_FULL_PATH}{new_file.file_unique_id}.{img.format}")
    img_file = f"{TEMP_DIR_FULL_PATH}{new_file.file_unique_id}.{img.format}"
    img.close()

    # duplicates checking starts here
    # img_hash = imagehash.average_hash(Image.open(img_file), hash_size=16)
    img_hash = imagehash.colorhash(Image.open(img_file), binbits=9)

    chat_id = str(update.message.chat.id)
    s_hash = str(img_hash)
    message_ids = mysql_check_similarity(chat_id, s_hash, update.message.message_id)

    if message_ids:
        for message_id in message_ids:
            await update.message.reply_text(f"Similar to https://t.me/c/{chat_id[4:]}/{message_id}\n")

    os.remove(img_file)


async def greet_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Greets new users in chats and announces when someone leaves"""
    result = extract_status_change(update.chat_member)
    if result is None:
        return

    was_member, is_member = result
    cause_name = update.chat_member.from_user.mention_html()
    member_name = update.chat_member.new_chat_member.user.mention_html()


    if not was_member and is_member:
        await update.effective_chat.send_message(
            f"{member_name} was added by {cause_name}. Welcome!",
            parse_mode=ParseMode.HTML,

        )

    elif was_member and not is_member:
        await update.effective_chat.send_message(
            f"{member_name} is no longer with us. Thanks a lot, {cause_name} ...",
            parse_mode=ParseMode.HTML,

        )

### start weather parser
@dataclass(slots=True, frozen=True)
class Coordinates:
    latitude: float
    longitude: float

def get_coordinates() -> Coordinates:
    """Returns current coordinates using IP address"""
    data = _get_ip_data()
    latitude = data['loc'].split(',')[0]
    longitude = data['loc'].split(',')[1]

    return Coordinates(latitude=latitude, longitude=longitude)

Celsius: TypeAlias = float


class WindDirection(IntEnum):
    North = 0
    Northeast = 45
    East = 90
    Southeast = 135
    South = 180
    Southwest = 225
    West = 270
    Northwest = 315


@dataclass(slots=True, frozen=True)
class Weather:
    location: str
    temperature: Celsius
    temperature_feeling: Celsius
    description: str
    wind_speed: float
    wind_direction: str
    sunrise: datetime
    sunset: datetime

def _parse_openweather_response(openweather_response: str) -> Weather:
    openweather_dict = json.loads(openweather_response)
    return Weather(
        location=_parse_location(openweather_dict),
        temperature=_parse_temperature(openweather_dict),
        temperature_feeling=_parse_temperature_feeling(openweather_dict),
        description=_parse_description(openweather_dict),
        sunrise=_parse_sun_time(openweather_dict, 'sunrise'),
        sunset=_parse_sun_time(openweather_dict, 'sunset'),
        wind_speed=_parse_wind_speed(openweather_dict),
        wind_direction=_parse_wind_direction(openweather_dict)
    )


def _parse_location(openweather_dict: dict) -> str:
    return openweather_dict['name']


def _parse_temperature(openweather_dict: dict) -> Celsius:
    return openweather_dict['main']['temp']


def _parse_temperature_feeling(openweather_dict: dict) -> Celsius:
    return openweather_dict['main']['feels_like']


def _parse_description(openweather_dict) -> str:
    return str(openweather_dict['weather'][0]['description']).capitalize()


def _parse_sun_time(openweather_dict: dict, time: Literal["sunrise", "sunset"]) -> datetime:
    return datetime.fromtimestamp(openweather_dict['sys'][time])


def _parse_wind_speed(openweather_dict: dict) -> float:
    return openweather_dict['wind']['speed']


def _parse_wind_direction(openweather_dict: dict) -> str:
    degrees = openweather_dict['wind']['deg']
    degrees = round(degrees / 45) * 45
    if degrees == 360:
        degrees = 0
    return WindDirection(degrees).name
### end weather parser

async def show_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message.text.replace('/weather ','')
    
    data = { 'appid':OPENWEATHER_APP_ID, 'q':message, 'units':'metric'}
    query = f"https://api.openweathermap.org/data/2.5/weather?{urllib.parse.urlencode(data)}"

    try:
        q = urllib.request.urlopen(query)
    except Exception as e:
        await update.effective_chat.send_message("😳")
        # weather debugger
        # context.error = e
        # await error_handler(update=update, context=context)
        return

    response = q.read()
    wthr = _parse_openweather_response(response)

    ret =  f'{wthr.location}, {wthr.description}\n' \
           f'Temperature is {wthr.temperature}°C, feels like {wthr.temperature_feeling}°C\n' \
           f'Wind: {wthr.wind_direction}, {wthr.wind_speed} m/s\n' \
           f'Sunrise: {wthr.sunrise.strftime("%H:%M")}\n' \
           f'Sunset: {wthr.sunset.strftime("%H:%M")}\n'

    await update.effective_chat.send_message(ret)

def main() -> None:
    """Run the bot."""
    application = Application.builder().token(TOKEN).build()
    application.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(greet_chat_members, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(MessageHandler(filters.PHOTO, image_handler))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("bad_command", bad_command))
    application.add_handler(CommandHandler("show_chats", show_chats))
    application.add_handler(CommandHandler("weather", show_weather))
    # application.add_handler(CommandHandler('location', request_location))
    # ...and the error handler
    application.add_error_handler(error_handler)

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
    
    