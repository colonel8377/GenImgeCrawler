#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目设置脚本 - 安装依赖和Playwright浏览器
"""

import subprocess
import sys
import os


def run_command(cmd, desc):
    """运行命令并显示结果"""
    print(f"\n{desc}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {desc} 成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {desc} 失败: {e}")
        print(f"错误输出: {e.stderr}")
        return False


def main():
    """主函数"""
    print("CivArchive爬虫 - 项目设置")
    print("=" * 50)

    # 检查Python版本
    if sys.version_info < (3, 8):
        print("错误: 需要Python 3.8或更高版本")
        sys.exit(1)

    # 安装Python依赖
    if not run_command("pip install -r requirements.txt", "安装Python依赖"):
        sys.exit(1)

    # 安装Playwright浏览器
    if not run_command("playwright install chromium", "安装Playwright Chromium浏览器"):
        sys.exit(1)

    print("\n" + "=" * 50)
    print("✓ 项目设置完成！")
    print("\n使用方法:")
    print("  python run.py --help              # 查看帮助")
    print("  python run.py --install-browser   # 重新安装浏览器")
    print("  python run.py --check-files       # 检查文件完整性")
    print("  python run.py --start-page 1      # 开始爬取")
    print("=" * 50)


if __name__ == "__main__":
    main()