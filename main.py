import asyncio
import tracemalloc

tracemalloc.start()

import sqlite3
import time

import requests
import xml.etree.ElementTree as ET

from telethon import TelegramClient
from telethon.sessions import StringSession

from configs import UPWORK_XML_URL, CHAT_ID, API_ID, API_HASH, BOT_TOKEN

bot = TelegramClient(StringSession(), API_ID, API_HASH).start(bot_token=BOT_TOKEN)


class UpworkXMLFilter:
    def __init__(self, xml_url: str, dbase_fname: str = 'test.db'):
        self.URL = xml_url
        self.xml = self.get_xml_()
        self.data = self.filter_data()
        sqliteConnection = sqlite3.connect(dbase_fname)
        self.cursor = sqliteConnection.cursor()
        self.new_data = self.check_data()

    def get_xml_(self):
        """Get XML

        Get request to URL, parse and return XML.

        :return:
        """

        r = requests.get(self.URL)
        print(f"Request status code: {r}")
        if r.status_code // 100 != 2:
            raise "Url Get Request Error"

        return ET.fromstring(r.content)

    def filter_data(self) -> dict:
        """Filter Data

        Filters XML data and extracts posts data.

        :return:
        """
        print('Starting filtering data...')
        posts_data = {}

        posts_ = self.xml.find("channel").findall("item")

        # Count posts
        posts_data["count"] = len(posts_)

        # Filter each post data.
        posts_data['posts'] = []
        for post_data in self.xml.find("channel").iter('item'):
            title: str = post_data.find("title").text
            link: str = post_data.find("link").text
            description: str = post_data.find("description").text
            pubdate: str = post_data.find('pubDate').text
            guid: str = post_data.find('guid').text

            post_dict: dict = {'title': title, 'link': link,
                               'description': description, 'pubDate': pubdate,
                               'guid': guid}

            posts_data['posts'].append(post_dict)
        # print(f"Data Filtered: {posts_data}")
        return posts_data

    def check_data(self) -> list:
        """Check Data

        Check for a new content by guid and return it.

        :return:
        """
        print('Check the data.')
        new_content = []
        select_guid = "SELECT guid FROM posts"
        self.cursor.execute(select_guid)
        rows = list(self.cursor.fetchall())

        for each_content in self.data['posts']:
            if each_content['guid'] not in rows:
                print("NOT IN DATABASE")
                new_content.append(each_content)
                print(f"DATA TO INSERT {each_content}")
                # Insert new posts database.
                insert_data = f"INSERT INTO posts (title, link, description, guid, pubDate)" \
                              f" VALUES ('{each_content['title']}', '{each_content['link']}', " \
                              f"'{each_content['description']}', '{each_content['guid']}', '{each_content['pubDate']}')"
                self.cursor.execute(insert_data)

        print('Data Has Been Checked.')

        return new_content


def check_for_updates():
    """Check For Updates.

    Checks upwork url for new updates and runs send_to_telegram() function.

    :return:
    """
    while True:
        time.sleep(2)
        new_posts = UpworkXMLFilter(xml_url=UPWORK_XML_URL).new_data
        # print(f"NEW POSTS: {new_posts}")
        if new_posts:
            bot.loop.run_until_complete(send_to_telegram(new_posts))


async def send_to_telegram(new_content: list[dict]) -> list:
    """Sends message to telegram.

    Sends message to Telegram Channel.

    :return:
    """

    await bot.connect()
    all_statuses = []

    for content in new_content:
        message = f"""<b>{content['title']}</b>\n\n
        {content['guid']}\n
        {content['description']}
        """.replace("</br>", "\n").replace("<br />", "\n").replace("</b>", "**").replace("<b>", "**")
        with open('messages.txt', 'w+') as f:
            f.write(f"{message}\n")
        # use token generated in first step
        time.sleep(5)
        print("\n\n\nSENDING MESSAGE\n\n\n")
        await bot.send_message(CHAT_ID, message)

    print("\n\n\nSENDING MESSAGE\n\n\n")
    return all_statuses


if __name__ == '__main__':
    print("1")
    bot.start()
    print("2")
    # bot.run_until_disconnected()
    print("3")
    check_for_updates()
