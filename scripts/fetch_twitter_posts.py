import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE = "https://api.twitter.com/2"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPOSITORY_ROOT / ".env"
OUTPUT = REPOSITORY_ROOT / "assets" / "twitter.json"


def load_local_env():
    if ENV_FILE.is_file():
        from dotenv import load_dotenv

        load_dotenv(ENV_FILE)


def get_json(path, token, params=None):
    url = f"{API_BASE}/{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Twitter API request failed ({error.code}): {detail}") from error


def main():
    load_local_env()
    username = os.environ.get("TWITTER_USERNAME", "").strip().lstrip("@")
    token = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
    if not username:
        raise SystemExit("Set the TWITTER_USERNAME repository variable.")
    if not token:
        raise SystemExit("Set the TWITTER_BEARER_TOKEN repository secret.")

    user_response = get_json(f"users/by/username/{username}", token)
    user = user_response.get("data")
    if not user or not user.get("id"):
        raise SystemExit(f"Twitter API did not return a user for @{username}.")

    tweets_response = get_json(
        f"users/{user['id']}/tweets",
        token,
        {
            "max_results": 10,
            "exclude": "replies,retweets",
            "tweet.fields": "created_at",
        },
    )
    result = {
        "username": user.get("username", username),
        "data": [
            {
                "id": tweet["id"],
                "text": tweet["text"],
                "created_at": tweet.get("created_at"),
            }
            for tweet in tweets_response.get("data", [])
        ],
    }

    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
