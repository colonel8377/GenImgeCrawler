# CivArchive 爬虫 (Playwright版本)

基于Playwright的并发爬虫，用于爬取CivArchive网站数据。

## 主要特性

- 🚀 **Playwright并发爬取**: 使用Playwright进行浏览器自动化，模拟真实用户行为
- 🎯 **智能路由拦截**: 自动拦截图片、视频、字体等资源，减少流量消耗
- 🔄 **断点续爬**: 支持断点续爬，程序中断后可继续从上次位置开始
- 📊 **状态管理**: 实时保存爬取状态和统计信息
- 🛡️ **代理支持**: 集成代理管理，支持自动切换代理节点
- 📁 **文件完整性检查**: 内置文件检查功能，确保数据完整性
- ⚡ **性能优化**: 自动调整并发数，根据系统资源优化性能

## 安装

### 1. 克隆项目
```bash
git clone <repository-url>
cd GenImgeCrawler
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 运行设置脚本
```bash
python setup.py
```

此脚本将自动安装Python依赖和Playwright浏览器。

## 使用方法

### 基本爬取
```bash
# 开始爬取（默认从第1页开始）
python run.py

# 指定起始页
python run.py --start-page 10

# 指定页码范围
python run.py --start-page 1 --end-page 100

# 设置并发数
python run.py --workers 3
```

### 参数说明

- `--start-page`: 起始页码 (默认: 1)
- `--end-page`: 结束页码 (默认: 自动尝试)
- `--workers`: 并发数 (默认: 3)
- `--no-resume`: 禁用断点续爬

### 故障恢复

爬虫具备完善的故障恢复机制：

- **网络连接失败**: 自动重试最多3次，每次间隔递增(1s→2s→4s)
- **代理切换**: 每次重试时自动切换代理节点
- **断点续爬**: 程序中断后可从上次位置继续
- **独立处理**: 单个页面失败不影响其他页面

## 架构特点

### 1. 极简设计
- **大幅简化代码**: crawler.py从577行减少到281行，proxy_manager.py从340行减少到81行
- **移除复杂逻辑**: 删除了build_id获取、评分系统、冷却期等复杂功能
- **独立处理**: 每个页面使用独立的浏览器实例，资源自动管理

### 2. 直接监听API
- 直接访问页面URL，无需预先获取buildId
- 监听网络请求捕获JSON数据
- 简单有效的API获取方式

### 3. 资源拦截优化
- 拦截图片、视频、字体等大文件
- 大幅减少网络流量消耗（减少90%+）
- 提升爬取速度和稳定性

### 4. 智能并发与重试
- 基于asyncio.Semaphore的简单并发控制
- 默认3并发，避免过载
- **智能重试机制**: 网络失败时自动重试最多3次
- **指数退避策略**: 重试间隔1s→2s→4s
- **自动代理切换**: 每次重试时切换代理节点
- 完善的错误处理，失败页面自动跳过

## 文件结构

```
GenImgeCrawler/
├── src/
│   ├── core/
│   │   └── crawler.py          # 主爬虫类 (Playwright版本)
│   ├── managers/
│   │   ├── proxy_manager.py    # 代理管理
│   │   ├── database_manager.py # 数据库管理
│   │   └── state_manager.py    # 状态管理
│   ├── utils/
│   │   ├── anti_crawl.py       # 反爬虫策略
│   │   └── logger.py           # 日志工具
│   └── config/
│       └── settings.py         # 配置管理
├── data/                       # 数据目录
├── requirements.txt            # Python依赖
├── setup.py                    # 安装脚本
└── run.py                      # 启动脚本
```

## 配置

编辑 `src/config/settings.py` 来修改配置：

```python
class CrawlerConfig:
    base_url: str = "https://civarchive.com"
    max_workers: int = 5          # 并发数
    request_timeout: int = 30     # 请求超时
    retry_count: int = 3          # 重试次数
    min_delay: float = 0.5        # 最小延迟
    max_delay: float = 2.0        # 最大延迟

class ProxyConfig:
    host: str = "127.0.0.1"       # 代理主机
    port: int = 7890              # 代理端口
    switch_api_url: str = "http://127.0.0.1:10809"  # 代理切换API
```

## 故障排除

### 常见问题

1. **浏览器启动失败**
   ```bash
   python run.py --install-browser
   ```

2. **代理连接失败**
   - 检查代理服务是否运行
   - 验证代理配置是否正确

3. **内存不足**
   - 降低并发数: `--workers 2`
   - 启用性能优化: `--optimize`

4. **爬取速度慢**
   - 调整代理设置
   - 检查网络连接
   - 增加并发数（如果系统允许）

### 日志查看

程序运行时会输出详细日志，包括：
- 爬取进度
- 错误信息
- 统计数据
- 性能指标

## 性能优化建议

1. **并发数设置**
   - 默认3并发，适合大多数情况
   - 根据网络质量调整：网络好可适当增加
   - 避免设置过高导致IP被封

2. **系统要求**
   - 推荐4GB+内存
   - 稳定的网络连接
   - SSD存储提升状态文件读写速度

3. **代理配置**
   - 使用高质量代理服务
   - 确保代理池有足够节点
   - 定期检查代理可用性

## 许可证

[请添加许可证信息]