(function () {
  const feed = document.getElementById("twitter-feed");
  if (!feed) return;

  function showMessage(message) {
    const paragraph = document.createElement("p");
    paragraph.className = "muted";
    paragraph.textContent = message;
    feed.replaceChildren(paragraph);
  }

  fetch(feed.dataset.feedUrl)
    .then(function (response) {
      if (!response.ok) throw new Error("Unable to load posts.");
      return response.json();
    })
    .then(function (payload) {
      if (!payload.username || !Array.isArray(payload.data) || !payload.data.length) {
        showMessage("No recent posts are available right now.");
        return;
      }

      const profile = document.createElement("a");
      profile.className = "twitter-profile";
      profile.href = "https://x.com/" + encodeURIComponent(payload.username);
      profile.target = "_blank";
      profile.rel = "noopener noreferrer";
      profile.textContent = "View @" + payload.username + " on X";

      const list = document.createElement("div");
      list.className = "twitter-post-list";
      payload.data.forEach(function (post) {
        const card = document.createElement("article");
        card.className = "twitter-card";

        const header = document.createElement("div");
        header.className = "twitter-card-header";

        const author = document.createElement("span");
        author.className = "twitter-author";
        author.textContent = "@" + payload.username;
        header.appendChild(author);

        if (post.created_at) {
          const date = document.createElement("time");
          date.className = "date";
          date.dateTime = post.created_at;
          date.textContent = new Intl.DateTimeFormat(undefined, {
            dateStyle: "medium",
            timeStyle: "short"
          }).format(new Date(post.created_at));
          header.appendChild(date);
        }

        const text = document.createElement("p");
        text.className = "twitter-text";
        text.textContent = post.text;

        const link = document.createElement("a");
        link.className = "twitter-post-link";
        link.href = "https://x.com/" + encodeURIComponent(payload.username) + "/status/" + encodeURIComponent(post.id);
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = "View post on X";

        card.append(header, text, link);
        list.appendChild(card);
      });

      feed.replaceChildren(list, profile);
    })
    .catch(function () {
      showMessage("Posts could not be loaded. Please try again later.");
    });
})();
