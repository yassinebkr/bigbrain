import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1280, "height": 800})
        
        print("Navigating to http://127.0.0.1:8080...")
        await page.goto("http://127.0.0.1:8080")
        print("Waiting for page to load and connect...")
        await asyncio.sleep(2)
        
        # Switch to python-dev workspace
        await page.click("button[data-workspace='python-dev']")
        await asyncio.sleep(1)
        
        # Type in the chat input
        print("Sending chat message...")
        await page.fill("#chat-input", "Write a python script to calculate the first 10 fibonacci numbers and print them.")
        await page.keyboard.press("Enter")
        
        print("Waiting 15 seconds for LLMs to generate and execute code...")
        await asyncio.sleep(15)
        
        print("Taking screenshot of live usage...")
        await page.screenshot(path="assets/frontend_live_usage.png")
        
        print("Done!")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
