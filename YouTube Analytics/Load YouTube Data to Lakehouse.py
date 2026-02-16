import requests, json, time, re
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# API_KEY = "<KEY>"
# CHANNEL_ID = "<CHANNEL_ID>"

BASE_URL = "https://www.googleapis.com/youtube/v3"
TABLE_NAME = "youtube_videos"

# ------------------------------------------
# 1. Get Upload Playlist ID
# ------------------------------------------
url = f"{BASE_URL}/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"
data = requests.get(url).json()

uploads_playlist = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

# ------------------------------------------
# 2. Get All Video IDs
# ------------------------------------------
video_ids = []
next_page = None

while True:
    url = f"{BASE_URL}/playlistItems?part=snippet&playlistId={uploads_playlist}&maxResults=50&pageToken={next_page or ''}&key={API_KEY}"
    res = requests.get(url).json()

    video_ids += [i["snippet"]["resourceId"]["videoId"] for i in res.get("items", [])]
    next_page = res.get("nextPageToken")

    if not next_page:
        break
    time.sleep(0.1)

# ------------------------------------------
# 3. Get Video Details (Batch API Calls)
# ------------------------------------------
videos = []
for i in range(0, len(video_ids), 50):
    batch = ",".join(video_ids[i:i+50])

    url = f"{BASE_URL}/videos?part=snippet,statistics,contentDetails,status&id={batch}&key={API_KEY}"
    res = requests.get(url).json()

    for v in res.get("items", []):
        sn = v.get("snippet", {})
        st = v.get("statistics", {})
        cd = v.get("contentDetails", {})
        ss = v.get("status", {})

        thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")

        videos.append({
            "video_id": v["id"],
            "title": sn.get("title"),
            "published_at": sn.get("publishedAt"),
            "channel_title": sn.get("channelTitle"),
            "view_count": int(st.get("viewCount", 0)),
            "like_count": int(st.get("likeCount", 0)),
            "comment_count": int(st.get("commentCount", 0)),
            "duration": cd.get("duration"),
            "privacy_status": ss.get("privacyStatus"),
            "thumbnail_url": thumb,
            "video_url": f"https://youtube.com/watch?v={v['id']}",
            "extracted_at": datetime.now().isoformat()
        })

    time.sleep(0.2)


# ------------------------------------------
# 4. Create Spark DataFrame
# ------------------------------------------
schema = StructType([
    StructField("video_id", StringType()),
    StructField("title", StringType()),
    StructField("published_at", StringType()),
    StructField("channel_title", StringType()),
    StructField("view_count", LongType()),
    StructField("like_count", LongType()),
    StructField("comment_count", LongType()),
    StructField("duration", StringType()),
    StructField("privacy_status", StringType()),
    StructField("thumbnail_url", StringType()),
    StructField("video_url", StringType()),
    StructField("extracted_at", StringType())
])

df = spark.createDataFrame(videos, schema)

# ------------------------------------------
# 5. Add Derived Columns
# ------------------------------------------
duration_udf = udf(
    lambda d: (
        (int(re.search(r'(\d+)H', d).group(1))*3600 if re.search(r'H', d or '') else 0) +
        (int(re.search(r'(\d+)M', d).group(1))*60 if re.search(r'M', d or '') else 0) +
        (int(re.search(r'(\d+)S', d).group(1)) if re.search(r'S', d or '') else 0)
    ) if d else 0,
    IntegerType()
)

df = (df
      .withColumn("published_date", to_date("published_at"))
      .withColumn("duration_seconds", duration_udf("duration"))
      .withColumn("engagement_rate",
          when(col("view_count") > 0,
               (col("like_count")+col("comment_count"))/col("view_count")*100)
          .otherwise(0))
)

# ------------------------------------------
# 6. Write to Lakehouse
# ------------------------------------------
df.write.mode("overwrite").saveAsTable(TABLE_NAME)
