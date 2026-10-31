---
layout: default
title: Home
---

<section class="hero">
  <p class="eyebrow">DATA PLATFORMS · CLOUD · ARCHITECTURE</p>
  <h1>Rabindra Jaiswal</h1>
  <h2>Data Architect &amp; Engineering Leader</h2>
  <p class="intro">I design and build modern data platforms, helping teams turn complex data systems into reliable, scalable products.</p>
  <p><a class="button primary" href="{{ '/resume/' | relative_url }}">Recent achievements</a> <a class="button secondary" href="{{ '/blog/' | relative_url }}">Read the blog</a></p>
  <p class="muted">Microsoft Fabric · Databricks · Snowflake · Spark · Python</p>
</section>
<section class="section">
  <p class="eyebrow">LATEST WRITING</p><h2>Recent articles</h2>
  {% for post in site.posts limit:5 %}
  <a class="post" href="{{ post.url | relative_url }}"><div><h3>{{ post.title }}</h3><p>{{ post.excerpt | strip_html | truncate: 160 }}</p></div><span class="date">{{ post.date | date: "%b %-d, %Y" }}</span></a>
  {% endfor %}
  <p><a href="{{ '/blog/' | relative_url }}">All articles →</a></p>
</section>
