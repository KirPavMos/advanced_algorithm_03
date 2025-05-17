import aiohttp
import asyncio
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
import requests

class BaseModel(ABC):
    @abstractmethod
    async def fetch_data(self, categories: List[str]) -> None:
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        pass

class WoysaClubParser(BaseModel):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(WoysaClubParser, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.data = {}
        self.base_url = "https://woysa.club"

    async def fetch_data_async(self, categories: List[str]) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_category_async(session, category) for category in categories]
            await asyncio.gather(*tasks)

    async def _fetch_category_async(self, session: aiohttp.ClientSession, category: str) -> None:
        try:
            url = f"{self.base_url}/{category}"
            async with session.get(url) as response:
                response.raise_for_status()
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                self.data[category] = self._parse_category(soup)
        except Exception as e:
            print(f"Ошибка при получении данных для категории {category}: {e}")
            self.data[category] = []

    def fetch_data_threaded(self, categories: List[str]) -> None:
        with ThreadPoolExecutor() as executor:
            executor.map(self._fetch_category_sync, categories)

    def _fetch_category_sync(self, category: str) -> None:
        try:
            url = f"{self.base_url}/{category}"
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            self.data[category] = self._parse_category(soup)
        except Exception as e:
            print(f"Ошибка при получении данных для категории {category}: {e}")
            self.data[category] = []

    def _parse_category(self, soup: BeautifulSoup) -> List[Dict]:

        articles = []
        for article in soup.find_all('article', limit=5):
            articles.append({
                'title': article.find('h2').text.strip() if article.find('h2') else 'No title',
                'content': article.find('p').text.strip() if article.find('p') else 'No content'
            })
        return articles

    async def fetch_data(self, categories: List[str]) -> None:
        await self.fetch_data_async(categories)

    def to_dict(self) -> Dict:
        return self.data


# Пример использования
async def main():
    parser = WoysaClubParser()

    # Асинхронная загрузка
    await parser.fetch_data_async(['#rec580600206', '#rec582709478'])
    print("Асинхронная загрузка завершена")
    print(parser.to_dict())

    # Многопоточная загрузка
    parser.fetch_data_threaded(['#rec581311284'])
    print("Многопоточная загрузка завершена")
    print(parser.to_dict())


if __name__ == "__main__":
    asyncio.run(main())