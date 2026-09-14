import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Use a reasonable desktop viewport
        page = await browser.new_page(viewport={"width": 1280, "height": 800})
        
        print("Navigating to http://127.0.0.1:8080...")
        await page.goto("http://127.0.0.1:8080")
        print("Waiting for page to load...")
        await asyncio.sleep(2)
        
        # Take a better home screenshot just in case
        await page.screenshot(path="/home/kwestog/bigbrain/assets/frontend_home.png")
        
        # Switch to python tab
        print("Switching to python-dev workspace...")
        await page.click('[data-workspace="python-dev"]')
        await asyncio.sleep(2)
        
        await page.screenshot(path="/home/kwestog/bigbrain/assets/frontend_python.png")
        print("Screenshot saved to /home/kwestog/bigbrain/assets/frontend_python.png")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
