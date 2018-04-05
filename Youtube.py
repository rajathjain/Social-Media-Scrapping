import time
from apiclient.discovery import build
import pandas as pd
import argparse
from pymongo import MongoClient
from datetime import datetime


client = MongoClient('localhost:27017')
db = client.Youtube
youtubes = db.you_recent

def credentials():
    DEVELOPER_KEY = "AIzaSyDM6QmCSRdJzQnm_61qLtkG_7AOw7PDwnc"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
    except:
        time.sleep(60*60)
        pass
    return (youtube)



def get_statstics(s, youtube=credentials()):
    videos_list_response = youtube.videos().list(
        id=s,
        part='id,statistics'
    ).execute()

    res = []
    for i in videos_list_response['items']:
        temp_res = dict(v_id=i['id'])
        temp_res.update(i['statistics'])
        res.append(temp_res)
    results = pd.DataFrame.from_dict(res)
    return (results)


def get_comment_threads(id):
    youtube=credentials()
    comment_results = youtube.commentThreads().list(
    part="snippet",
    videoId=id,
    textFormat="plainText"
    ).execute()
    return comment_results["items"]


def get_comments(video_id):
    try:
        res = get_comment_threads(video_id)
        comment = []
        # for x in range(len(res)):
        #     comment.append((res[x]['snippet']['topLevelComment']['snippet']['textDisplay'],
        #                     res[x]['snippet']['topLevelComment']['snippet']['likeCount']))
        comment=[(res[x]['snippet']['topLevelComment']['snippet']['textDisplay'],res[x]['snippet']['topLevelComment']['snippet']['likeCount'])
                 for x in len(res)]
    except:
        comment = ['Disabled comments']
    return (comment)


def video_info(video_list):
    no_videos = len(video_list)
    for i in range(0, no_videos, 50):
        fifty_videos = []
        print(i,i+50)
        if i+50<no_videos:
            end=i+50
        else:
            end=i+(no_videos%50)
        for j in range(i, end):
            fifty_videos.append(video_list[j])
            s = ','.join(fifty_videos)
        try:
            results = get_statstics(s)
        except:
            pass
        for i in range(0, len(results.v_id)):
            comment = get_comments(results.v_id[i])
            view_count = youtubes.find_one({"_id": results.v_id[i]},
                                           {"viewCount": 1, "commentCount": 1, "dislikeCount": 1, "likeCount": 1,
                                            "favoriteCount": 1, "Comment": 1,"Time":1 ,"Total_Sentiment":1,"Sentiment_Quotient":1,"_id": 0})
            view_count['viewCount'].append(results.viewCount[i])
            view_count['commentCount'].append(results.commentCount[i])
            view_count['dislikeCount'].append(results.dislikeCount[i])
            view_count['likeCount'].append(results.likeCount[i])
            view_count['favoriteCount'].append(results.favoriteCount[i])
            view_count['Total_Sentiment'].append(float(results.likeCount[i])-float(results.dislikeCount[i]))
            view_count['Sentiment_Quotient'].append((float(results.likeCount[i])-float(results.dislikeCount[i]))/(float(results.viewCount[i])+1))
            view_count['Time'].append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            view_count['Comment'] = comment
            youtubes.update_one({"_id": results.v_id[i]}, {
                '$set': {"viewCount": view_count['viewCount'], "commentCount": view_count['commentCount'],
                         "likeCount": view_count['likeCount'],
                         "dislikeCount": view_count['dislikeCount'], "favoriteCount": view_count['favoriteCount'],
                         "comment": view_count['Comment'],"Time":view_count['Time'],"Total_Sentiment":view_count['Total_Sentiment'],"Sentiment_Quotient":view_count['Sentiment_Quotient']}})


def youtube_search(k):
    #if __name__ == "__main__":
    # argparser = argparse.ArgumentParser(conflict_handler='resolve')
    # argparser.add_argument("--q", help="Search term", default="TDP")
    # argparser.add_argument("--max-results", help="Max results", default=50)
    # args = argparser.parse_args()
    # options = args

    youtube = credentials()
    search_response = youtube.search().list(
        q=k,
        type="video",
        order="date",
        part="id,snippet",
        maxResults=50
    ).execute()

    # Call the search.list method to retrieve results matching the specified
    # query term.

    videos = {}

    # Add each result to the appropriate list, and then display the lists of
    # matching videos.
    # Filter out channels, and playlists.
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            # videos.append("%s" % (search_result["id"]["videoId"]))
            videos[search_result["id"]["videoId"]] = search_result["snippet"]["title"]
    s = ','.join(videos.keys())

    videos_list_response = youtube.videos().list(
        id=s,
        part='id,statistics'
    ).execute()

    res = []
    for i in videos_list_response['items']:
        temp_res = dict(v_id=i['id'], v_title=videos[i['id']])
        temp_res.update(i['statistics'])
        res.append(temp_res)

    # print "Videos:\n", "\n".join(videos), "\n"

    nextPageToken = search_response.get('nextPageToken')
    if nextPageToken:
        next_page_response=youtube.search().list(
            q=k,
            type="video",
            pageToken=nextPageToken,
            order="date",
            part="id,snippet",
            maxResults=50
        ).execute()

        videos = {}

        for search_result in next_page_response.get("items", []):
            if search_result["id"]["kind"] == "youtube#video":
                if search_result["id"]["videoId"] not in videos.keys():
                    videos[search_result["id"]["videoId"]] = search_result["snippet"]["title"]
        # videos_list_response['items'].sort(key=lambda x: int(x['statistics']['likeCount']), reverse=True)
        # res = pd.read_json(json.dumps(videos_list_response['items']))

        s = ','.join(videos.keys())

        videos_list_response = youtube.videos().list(
            id=s,
            part='id,statistics'
        ).execute()


        for i in videos_list_response['items']:
            temp_res = dict(v_id=i['id'], v_title=videos[i['id']])
            temp_res.update(i['statistics'])
            res.append(temp_res)
    else:
        pass
    results = pd.DataFrame.from_dict(res)
    return(results)


j=0
keyword_list=['TDP','BJP']
while True:
    you={}
    print('Start')
    try:
        key_id = youtubes.find({}, {"_id": 1})
    except:
        pass
    #youtube_id=[]
    # for x in key_id:
    #     youtube_id.append(x['_id'])
    youtube_id=[]
    youtube_id=[x["_id"] for x in key_id if key_id!=[]]
    for k in keyword_list:
        try:
            results=youtube_search(k)
            print(k)
            for i in range(0,len(results.v_id)):
                if results.v_id[i] in youtube_id:
                    pass
                else:
                        comment=get_comments(results.v_id[i])
                        you[results.v_id[i]]={"_id":results.v_id[i],"name":results.v_title[i],'viewCount':[results.viewCount[i]],'commentCount':[results.commentCount[i]],'dislikeCount':[results.dislikeCount[i]],
                                              'likeCount':[results.likeCount[i]],'favoriteCount':[results.favoriteCount[i]],'Comment':comment,'Time':[datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                                               "Total_Sentiment":[float(results.likeCount[i])-float(results.dislikeCount[i])],"Sentiment_Quotient":[(float(results.likeCount[i])-float(results.dislikeCount[i]))/(float(results.viewCount[i])+1)]}
         except:
             print("Error")
             time.sleep(60)
             pass
    try:
        youtubes.insert(you.values())
    except:
        pass
    video_info(youtube_id)
    j = j + 1
    print(j)
    print('Stop')
    time.sleep(60*60)



