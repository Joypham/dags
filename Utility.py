from dateutil.parser import parse
import telegram
import time


class Utility:
    @staticmethod
    def send_telegram_message(token, list_chat_id, message):
        bot = telegram.Bot(token=token)
        for chat_id in list_chat_id:
            bot.send_message(text=message, chat_id=chat_id)

    @staticmethod
    def to_int(value):
        try:
            return int(value)
        except ValueError:
            return 0

    @staticmethod
    def current_timestamp():
        return int(time.time())

    @staticmethod
    def date_string_to_timestamp(string):
        try:
            return time.mktime(parse(string).timetuple())
        except ValueError:
            return False
