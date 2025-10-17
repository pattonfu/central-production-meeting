## 1. 使用说明：

### 1.1 安装

```bash
pip install -r requirements.txt
```

### 1.2 配置

在运行脚本前，需要修改resources里面的`cookie.txt`和`csrftoken.txt`文件，分别存放从浏览器中获取的`cookie`和`x-csrftoken`。
其中query.txt里面是需要查询的DQL语句，一般不需要修改。

### 1.3 运行

```bash
python fetch_dynatrace_records.py
```

## 2. 功能说明：

该脚本用于从Dynatrace平台获取指定时间（过去7天）范围内的监控数据（错误spans），并将数据保存为excel文件（表格内容几乎接近
`Production meeting`里面的`Common Errors`部分。
详细功能如下：

### 2.1 初始化：

构建今日与昨日目录：output/{TODAY_STR} 与 output/{LAST_DAY_STR}。
创建日志文件。

### 2.2 读取资源：

从 resources/query.txt、resources/cookie.txt、resources/csrftoken.txt 获取查询参数与认证信息。

### 2.3 时间窗口计算：

* end_time = 当前时间。
* start_time = end_time - 7 天。
* 循环 7 次，每次发送 1 天时间范围的 DQL 请求。

### 2.4 make_request：

* POST 执行 DQL，获取 requestToken。
* 轮询状态（最长 6 分钟，间隔 10 秒）。
* 成功后提取 result.records 写入 dql_result_for_day_{i}.json。

### 2.5 handle_data：

* 读取当天执行完后的合并后的总文件 output/{TODAY_STR}/dql_result_for_7_days_{TODAY}.json。
* 读取昨日抓取的（output/{LAST_DAY_STR}）对应 7 日聚合文件与第 7 天文件，用于对比。
* 第一轮：按 span.events.exception.message 聚合同类，累计：
    * total_count（本次 7 日总次数）
    * pre_total_count（昨日 7 日数据对应消息的数量，用于对比）
    * quantity_for_previous_day（本次第 7 天数据）
    * pre_quantity_for_previous_day（昨日第 7 天同消息数量）
    * apps、stacktraces（集合并去重）
    * is_new（昨日不存在但今日出现的消息标记）
* 第二轮：应用 FUZZY_RULES 正则做归类（使用 re.fullmatch，完全匹配）。
    * 如果需要加入或删除聚合规则，修改 FUZZY_RULES 列表即可。
* 输出为列表后写入 Excel：字段包括 app、归类后的异常模式、原始消息集合、堆栈集合、数量与前值等。

### 2.6 生成报表：

* output/{TODAY_STR}/summary.xlsx