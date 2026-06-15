---
name: news_briefing
when_to_use: When the user asks for a news update, a briefing, or what's happening with a topic.
allowed_tools: [web_search]
required: [request]
---

Give a short spoken news briefing on the user's topic.

1. Use `web_search` to find the few most recent, important developments on the topic.
2. Summarise them into three or four spoken sentences — plain language, no lists,
   no markdown, numbers as words.
3. Lead with the single most important item. If nothing notable turned up, say so
   briefly rather than padding.
