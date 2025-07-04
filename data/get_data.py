from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from bs4 import BeautifulSoup
import httpx
import pandas as pd
import asyncio
import re

base_url = "https://ecobici.cdmx.gob.mx/"
req = httpx.get(base_url + "datos-abiertos/")
soup = BeautifulSoup(req.text, "html.parser")
links = soup.find_all("a", href=True)
uselinks = [link["href"] for link in links if "wp-content" in link["href"]]
dates = [
    (
        re.search(r"\d{4}[-_]{0,1}(\d{2}|dic|ene|nov|oct)", link).group()
        if link is not None
        else None
    )
    for link in uselinks
]


async def get_data(date, link, client):
    CHUNKSIZE = 1024
    url = base_url + (link[1:] if link.startswith("/") else link)
    if not url.endswith(".csv"):
        print(f"Skipping {url} as it is not a CSV file.")
        return
    try:
        async with client.stream("GET", url) as req:
            total = int(req.headers.get("Content-Length", 0))
            pb = tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                desc=f"Downloading {date}",
            )

            with open("data/historic/ecobici_" + date + ".csv", "wb+") as file:
                async for chunk in req.aiter_bytes(chunk_size=CHUNKSIZE):
                    if chunk:
                        pb.update(len(chunk))
                        file.write(chunk)
            pb.close()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error for {url}: {e} damn it")
    except httpx.RequestError as e:
        print(f"Request error for {url}: {e}")
    except httpx.ReadError as e:
        print(f"Read error for {url}: {e}")
    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")


async def main():
    print("Starting to gather data")
    async with httpx.AsyncClient() as client:
        await tqdm_asyncio.gather(
            *[get_data(date, link, client) for date, link in zip(dates, uselinks)]
        )
    print("Data collected")


asyncio.run(main())
