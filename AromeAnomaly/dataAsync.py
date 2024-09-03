import os
import asyncio
import aiohttp
from datetime import datetime

async def download_grib_025(date):  # date format : 2024-08-26T06:00:00Z
    prevision_list = ['00H06H','07H12H','13H18H','19H24H','25H30H','31H36H','37H42H','43H48H']
    base_url = "https://object.data.gouv.fr/meteofrance-pnt/pnt"
    base_path = "C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data"
    
    # Ensure the base directory exists
    os.makedirs(os.path.join(base_path, date.replace(':', '-')), exist_ok=True)

    async def download_file(session, prevision):
        url = f"{base_url}/{date}/arome/0025/SP1/arome__0025__SP1__{prevision}__{date}.grib2"
        filename = f"arome__0025__SP1__{prevision}__{date.replace(':', '-')}.grib2"
        file_path = os.path.join(base_path, date.replace(':', '-'), filename)
        
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    await asyncio.to_thread(write_file, file_path, content)
                else:
                    print(f"Failed to download {url}. Status code: {response.status}")
        except Exception as e:
            print(f"An error occurred while downloading {url}: {e}")

    def write_file(file_path, content):
        with open(file_path, "wb") as f:
            f.write(content)

    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, prevision) for prevision in prevision_list]
        await asyncio.gather(*tasks)

# Run the asynchronous function
if __name__ == "__main__":
    date = datetime.today().strftime('%Y-%m-%d') + 'T09:00:00Z'
    asyncio.run(download_grib_025(date))