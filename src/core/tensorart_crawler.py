#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import random
import json
from playwright.async_api import async_playwright, Page, Response

from src.config.settings import TensorDatabaseConfig, ProxyConfig
from src.managers.TensorDatabaseManager import TensorDatabaseManager
from src.managers.proxy_manager import ProxyNodeManager

# 确保 tensor_database.py 在同一目录下或正确引用

from src.utils import get_logger

logger = get_logger(__name__)


class TensorArtCrawler:
    def __init__(self, headless=True, db_path=TensorDatabaseConfig.db_path):
        self.headless = headless
        self.target_url = "https://tensor.art/zh/models?sort=LATEST_UPDATE"
        # 监听的 API 特征路径
        self.api_pattern = "project/portal/list/v3"
        self.db = TensorDatabaseManager(db_path=db_path)
        self.is_running = True
        self.proxy_mgr = ProxyNodeManager()

    async def _handle_response(self, response: Response):
        """处理拦截到的网络响应"""
        # 1. 过滤 URL
        if self.api_pattern not in response.url:
            return

        # 2. 过滤状态码
        if response.status != 200:
            return

        # 3. 解析数据
        try:
            # Playwright 解析 JSON 可能会因为页面关闭而报错，需捕获
            json_data = await response.json()

            # 校验业务 Code
            if str(json_data.get("code")) != "0":
                return

            items = json_data.get("data", {}).get("items", [])
            if items:
                count = self.db.batch_save(items)
                logger.info(f"✨ 捕获数据: {len(items)} 条 | 入库: {count} 条")
            else:
                # 有时候 API 返回空 items，可能是到底了或者加载中
                logger.debug("API 返回空数据")

        except Exception as e:
            # 忽略一些无关紧要的解析错误
            pass

    async def _mimic_human_behavior(self, page: Page):
        """
        模仿人类的浏览行为：
        - 大部分时间向下滑动加载数据
        - 偶尔向上滑动（回看）
        - 随机停顿
        """
        # 1. 主滚动动作：大幅向下滑动 (为了触发加载)
        # 随机滚动 800 - 1500 像素
        scroll_down_main = random.randint(1200, 2000)
        await page.mouse.wheel(0, scroll_down_main)

        # 这里的等待相当于人眼在扫描屏幕
        await asyncio.sleep(random.uniform(0.8, 1.5))

        # 2. 微调动作：模拟"浏览"状态
        # 进行 1-3 次微小的修正滚动
        sub_actions = random.randint(1, 3)

        for _ in range(sub_actions):
            # 生成一个 0.0 到 1.0 的随机数
            action_prob = random.random()

            if action_prob < 0.3:
                # 30% 概率：向上回滚 (模拟"看一眼上面的图")
                # 负数代表向上
                scroll_up_small = -random.randint(100, 400)
                await page.mouse.wheel(0, scroll_up_small)
                # logger.debug(f"👀 模拟回看: {scroll_up_small}px")

            else:
                # 70% 概率：继续缓慢向下滑动 (模拟"正在阅读")
                scroll_down_small = random.randint(100, 400)
                await page.mouse.wheel(0, scroll_down_small)

            # 每次微操作后的短暂亦或
            await asyncio.sleep(random.uniform(0.3, 0.8))

        # 3. 强制触底检查 (确保触发无限加载)
        # 偶尔执行一次 JS 滚动到底部，防止 mouse.wheel 没滑够导致 API 不触发
        if random.random() < 0.4:
            await page.evaluate("window.scrollBy(0, 300)")

    async def run(self):
        logger.info("启动 Playwright 爬虫 (仿人行为模式)...")

        async with async_playwright() as p:
            # 启动浏览器
            proxy = {
                "server": self.proxy_mgr.get_proxy(),
            }
            browser = await p.chromium.launch(
                headless=self.headless,
                proxy=proxy,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled"  # 稍微隐藏一下自动化特征
                ]
            )

            # 创建上下文 (设置较大的视口，更容易触发加载)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )

            page = await context.new_page()

            # --- 1. 资源拦截 (核心优化) ---
            # 屏蔽图片、媒体、字体，大幅提升滚屏流畅度和节省流量
            await page.route("**/*", lambda route:
            route.abort()
            if route.request.resource_type in ["image", "media"]
            else route.continue_()
                             )

            # --- 2. 注册 API 监听 ---
            page.on("response", self._handle_response)

            try:
                logger.info(f"打开页面: {self.target_url}")
                await page.goto(self.target_url, wait_until="domcontentloaded", timeout=60000)

                # 等待初始内容加载
                await asyncio.sleep(5)

                logger.info("开始模拟人类浏览行为...")

                # --- 3. 循环滚动 ---
                while self.is_running:
                    # 执行拟人化滚动逻辑
                    await self._mimic_human_behavior(page)

                    # 每一大轮操作后，随机休息一下
                    # 不要太快，否则 API 请求过快可能被封 IP
                    await asyncio.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                logger.error(f"爬虫运行异常: {e}")
            finally:
                logger.info("停止爬虫，关闭浏览器...")
                await context.close()
                await browser.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--headless', default=False, action='store_true', help="使用无头模式运行")
    args = parser.parse_args()

    crawler = TensorArtCrawler(headless=args.headless)
    try:
        asyncio.run(crawler.run())
    except KeyboardInterrupt:
        logger.info("用户主动停止")