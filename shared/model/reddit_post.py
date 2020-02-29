"""Import dataclasses annotation."""
from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class RedditPost:
    """Data class for reddit posts."""

    url: str
    author_name: str
    subreddit_name: str
    post_text: str
    post_text_emb: List[List]
    number_of_votes: int
    is_NSFW: bool
    number_of_comments: int
