"""演示数据源：本地模拟「外部资讯 API」，供抓取链路端到端跑通。

为什么需要它：
  课堂现场若直接抓取真实新闻站点，存在反爬、网络抖动、页面改版等风险。
  这里提供一个本地 HTTP 接口返回可控内容，抓取端仍然走真实的 httpx 请求，
  因此「抓取 → Broker → 入库 → 向量化」链路是真实执行的，只是内容可控。
  演示时也可在「数据源」页自行添加真实站点 URL。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Module
from app.db.session import get_session

router = APIRouter(prefix="/api/demo", tags=["demo"])

# 各模块的示例情报（真实感为主，覆盖新闻/政治/商业统计/金融）
_FEEDS: dict[str, list[dict]] = {
    "news": [
        {
            "title": "国内头部电机厂商发布新一代永磁同步电机，效率提升至 97.5%",
            "content": "该系列电机采用新型稀土永磁材料与扁线绕组工艺，额定效率由 96.2% 提升至 97.5%，"
            "在新能源汽车主驱场景下可降低整车百公里电耗约 3%。厂商计划于四季度量产，"
            "已获得两家整车厂定点订单。业内认为，扁线+高牌号硅钢的组合将成为未来两年主流路线。",
            "author": "行业快讯",
            "tags": ["永磁同步电机", "新能源", "量产"],
        },
        {
            "title": "工业电机能效新国标实施在即，中小厂商面临产线改造压力",
            "content": "新版电机能效标准将IE3定为最低准入等级，IE4及以上产品在政府采购中优先。"
            "测算显示，中小电机企业平均需投入约 800 万至 1500 万元改造绕线、冲片与测试产线。"
            "部分厂商选择外购高牌号硅钢与高效冲片以缩短改造周期。",
            "author": "产业观察",
            "tags": ["能效标准", "IE4", "产线改造"],
        },
        {
            "title": "人形机器人关节电机需求爆发，微型无框力矩电机交期拉长至 20 周",
            "content": "多家机器人厂商加大关节模组采购，带动微型无框力矩电机与空心杯电机需求快速增长。"
            "上游供应商产能利用率已超 95%，标准品交期由 8 周延长至 20 周。"
            "部分厂商开始锁定全年产能，并推动国产编码器与驱动器替代。",
            "author": "快讯",
            "tags": ["人形机器人", "无框力矩电机", "交期"],
        },
    ],
    "politics": [
        {
            "title": "工信部发布工业能效提升行动计划，高效电机被列为重点方向",
            "content": "行动计划提出到 2027 年，重点用能行业高效电机使用比例提升至 70% 以上，"
            "并将高效电机及控制系统纳入绿色制造专项资金支持范围。"
            "同时明确对淘汰类落后电机产能实施差别化电价，倒逼存量替换。",
            "author": "政策速递",
            "tags": ["工信部", "能效", "补贴"],
        },
        {
            "title": "稀土出口管理政策调整，永磁材料成本预期波动加大",
            "content": "相关部门对部分稀土冶炼分离产品加强出口管理，市场预期中重稀土价格短期偏强。"
            "由于钕铁硼占永磁电机材料成本约 30%-40%，下游整机厂开始评估"
            "与磁材厂签订长协或推动减少重稀土用量的晶界扩散工艺。",
            "author": "政策速递",
            "tags": ["稀土", "出口管理", "磁材"],
        },
        {
            "title": "多国推进碳关税机制，电机产品出口需补充碳足迹声明",
            "content": "欧盟碳边境调节机制过渡期要求进口商申报产品隐含碳排放。"
            "国内电机出口企业需建立从硅钢、铜线到整机的碳足迹核算体系，"
            "部分头部厂商已取得第三方核查声明以维持出口竞争力。",
            "author": "合规观察",
            "tags": ["碳关税", "CBAM", "出口"],
        },
    ],
    "business": [
        {
            "title": "上半年中小型电机产量同比增长 6.8%，高效机型占比首次过半",
            "content": "统计数据显示，上半年国内中小型电机产量约 1.42 亿千瓦，同比增长 6.8%；"
            "其中 IE3 及以上高效机型占比达 52.3%，较去年同期提升 9.1 个百分点。"
            "新能源汽车驱动电机产量同比增长 21%，为主要增量来源。",
            "author": "统计月报",
            "tags": ["产量", "高效电机", "新能源"],
        },
        {
            "title": "主要原材料价格分化：硅钢走弱、铜价高位震荡、稀土偏强",
            "content": "无取向硅钢均价环比下降约 4%，铜价维持高位震荡，"
            "镨钕氧化物价格环比上涨约 6%。综合测算，标准 IE3 电机单位成本环比基本持平，"
            "但高效机型因磁材用量提升，成本压力略有上升。",
            "author": "成本监测",
            "tags": ["硅钢", "铜价", "稀土价格"],
        },
        {
            "title": "电机行业出口额同比增长 11.4%，东南亚与欧洲为主要增长市场",
            "content": "上半年电机产品出口额同比增长 11.4%，其中对东南亚出口增长 18%，"
            "对欧洲增长 9%。高效电机与伺服系统出口单价分别提升 5% 与 7%。"
            "汇率波动与海运成本是影响下半年出口利润的主要变量。",
            "author": "外贸统计",
            "tags": ["出口", "东南亚", "欧洲"],
        },
    ],
    "finance": [
        {
            "title": "两家电机龙头发布业绩预告，毛利率环比改善",
            "content": "受益于产品结构向高效机型倾斜与原材料价格回落，两家头部电机企业"
            "二季度毛利率环比提升 1.2 至 1.8 个百分点。机构预计下半年"
            "在能效政策驱动下，高效电机出货占比有望继续提升。",
            "author": "公司公告",
            "tags": ["业绩预告", "毛利率"],
        },
        {
            "title": "电机板块获机构加仓，资金关注能效与机器人双主线",
            "content": "近一月电机制造板块获得多家机构调研与增持，"
            "资金主要关注两条主线：一是能效标准驱动的存量替换，二是人形机器人带动的"
            "微特电机增量。板块估值处于近三年中枢偏上水平。",
            "author": "资金流向",
            "tags": ["机构调研", "板块", "估值"],
        },
        {
            "title": "某电机企业完成 6 亿元定增，用于高效电机与伺服产线扩建",
            "content": "公司公告完成 6 亿元定向增发，募集资金用于高效电机智能产线、"
            "伺服系统研发中心及补充流动资金。项目达产后预计新增年产能 260 万千瓦，"
            "重点面向工业节能与机器人应用。",
            "author": "公司公告",
            "tags": ["定增", "产能扩张", "伺服"],
        },
    ],
}


@router.get("/feed/{module_code}")
async def demo_feed(module_code: str, session: AsyncSession = Depends(get_session)) -> dict:
    """按模块返回示例资讯（模拟外部 API）。"""
    if module_code not in _FEEDS:
        raise HTTPException(404, f"未知模块：{module_code}")

    module = (
        await session.execute(select(Module).where(Module.module_code == module_code))
    ).scalar_one_or_none()
    module_id = module.module_id if module else 1

    now = datetime.now(timezone.utc)
    items = []
    for idx, raw in enumerate(_FEEDS[module_code]):
        items.append(
            {
                "title": raw["title"],
                "content": raw["content"],
                "url": f"https://demo.domel.local/{module_code}/{idx + 1}",
                # 让时间分散在最近几天，便于报告按时间窗聚合
                "publish_time": (now - timedelta(hours=idx * 7 + 1)).isoformat(),
                "module_id": module_id,
                "author": raw.get("author"),
                "tags": raw.get("tags", []),
                "language": "zh",
            }
        )
    return {"items": items}
