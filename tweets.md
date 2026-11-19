---
layout: default
title: Recent Tweets
permalink: /tweets/
description: Recent posts from X (formerly Twitter).
---
<section class="section tweets-section">
  <p class="eyebrow">ON X</p>
  <h1>Recent posts</h1>
  <p class="muted">My latest posts from X (formerly Twitter).</p>
  <div id="twitter-feed" class="twitter-feed" data-feed-url="{{ '/assets/twitter.json' | relative_url }}" aria-live="polite">
    <p class="muted">Loading posts…</p>
  </div>
</section>
<script src="{{ '/assets/twitter.js' | relative_url }}" defer></script>
