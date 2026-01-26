#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速启动脚本
"""

import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.core.crawler import main

if __name__ == "__main__":
    main()
