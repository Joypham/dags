import telegram


class Utility:
    @staticmethod
    def send_telegram_message(token, list_chat_id, message):
        bot = telegram.Bot(token=token)
        for chat_id in list_chat_id:
            bot.send_message(text=message, chat_id=chat_id)
