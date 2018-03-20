import time
from apiclient.discovery import build
import pandas as pd
import argparse
from pymongo import MongoClient

client = MongoClient('localhost:27017')
db = client.Youtube
youtubes = db.you_videos

def youtube():
    DEVELOPER_KEY = "AIzaSyDM6QmCSRdJzQnm_61qLtkG_7AOw7PDwnc"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    if __name__ == "__main__":
    argparser = argparse.ArgumentParser(conflict_handler='resolve')
    argparser.add_argument("--q", help="Search term", default='TDP')
    argparser.add_argument("--max-results", help="Max results", default=50)
    args = argparser.parse_args()
    options = args

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
    search_response = youtube.search().list(
        q=options.q,
        type="video",
        part="id,snippet",
        maxResults=options.max_results
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

    # print "Videos:\n", "\n".join(videos), "\n"

    s = ','.join(videos.keys())

    videos_list_response = youtube.videos().list(
        id=s,
        part='id,statistics'
    ).execute()

    # videos_list_response['items'].sort(key=lambda x: int(x['statistics']['likeCount']), reverse=True)
    # res = pd.read_json(json.dumps(videos_list_response['items']))

    res = []
    for i in videos_list_response['items']:
        temp_res = dict(v_id=i['id'], v_title=videos[i['id']])
        temp_res.update(i['statistics'])
        res.append(temp_res)
    results = pd.DataFrame.from_dict(res)
    return(results)

def get_comment_threads(id):
    DEVELOPER_KEY = "AIzaSyDM6QmCSRdJzQnm_61qLtkG_7AOw7PDwnc"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
    comment_results = youtube.commentThreads().list(
    part="snippet",
    videoId=id,
    textFormat="plainText"
    ).execute()
    return comment_results["items"]


j=0
while True:
    print('Start')
    results=youtube()
    key = youtubes.find({}, {"_id": 1})
    youtube_id = []
    for x in key:
        youtube_id.append(x['_id'])
    you = {}
    for i in range(0,len(results.v_id)):
    #     try:
    #         res = get_comment_threads(results.v_id[i])
    #         comment = []
    #         for x in range(len(res)):
    #             comment.append((res[x]['snippet']['topLevelComment']['snippet']['textDisplay'],
    #                             res[x]['snippet']['topLevelComment']['snippet']['likeCount']))
    #     except:
    #         comment=['Disabled comments']
    # # key = youtubes.find({}, {"_id": 1})
    # youtube_id=[]
    # for x in key:
    #     youtube_id.append(x['_id'])
    # you={}
    #for i in range(0, len(results.v_id)):
        try:
            res = get_comment_threads(results.v_id[i])
            comment = []
            for x in range(len(res)):
                comment.append((res[x]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                res[x]['snippet']['topLevelComment']['snippet']['likeCount']))
        except:
            comment = ['Disabled comments']

        if results.v_id[i] in youtube_id:
                view_count = youtubes.find_one({"_id": results.v_id[i]}, {"viewCount": 1, "commentCount":1,"dislikeCount":1,"likeCount":1,"favoriteCount":1,"Comment":1,"_id": 0})
                view_count['viewCount'].append(results.viewCount[i])
                view_count['commentCount'].append(results.commentCount[i])
                view_count['dislikeCount'].append(results.dislikeCount[i])
                view_count['likeCount'].append(results.likeCount[i])
                view_count['favoriteCount'].append(results.favoriteCount[i])
                view_count['Comment']=comment
                youtubes.update_one({"_id": results.v_id[i]}, {'$set': {"viewCount": view_count['viewCount'],"commentCount":view_count['commentCount'],"likeCount":view_count['likeCount'],
                                                                       "dislikeCount": view_count['dislikeCount'],"favoriteCount":view_count['favoriteCount'],"comment":view_count['comment']}})
            else:
                you[results.v_id[i]]={"_id":results.v_id[i],"name":results.v_title[i],'viewCount':[results.viewCount[i]],'commentCount':[results.commentCount[i]],'dislikeCount':[results.dislikeCount[i]],'likeCount':[results.likeCount[i]],'favoriteCount':[results.favoriteCount[i]],'Comment':comment}


    # results=results.drop(columns=['commentCount','dislikeCount','favoriteCount','likeCount','v_title'])
    # data=pd.merge(data,results,on='v_id',how='outer')
    # print(data)

    j=j+1
    print(j)
    print('Stop')
    try:
        youtubes.insert(you.values())
    except:
        pass
    #time.sleep(15)


#Comments
# if len(you['hQyeNBDCrsA']['viewCount'])>1:
#     if ((int(you['hQyeNBDCrsA']['viewCount'][-2]) - int(you['hQyeNBDCrsA']['viewCount'][-1])) / int(
#             you['hQyeNBDCrsA']['viewCount'][-2])) > 0.02:
#         print('Video hQyeNBDCrsA is going Viral')
#     else:
#         print('Everything seems fine')
#     if ((int(you['VtjQ1oM9A-E']['viewCount'][-2]) - int(you['VtjQ1oM9A-E']['viewCount'][-1])) / int(
#             you['VtjQ1oM9A-E']['viewCount'][-2])) > 0.02:
#         print('Video VtjQ1oM9A-E is going Viral')
#     else:
#         print('Everything seems fine')
# data = json.dumps(you)
# with open("you.json", "w") as f:
#     f.write(data)


#  you[results.v_id[i]]['viewCount'].append(results.viewCount[i])
#  you[results.v_id[i]]['commentCount'].append(results.commentCount[i])
#  you[results.v_id[i]]['dislikeCount'].append(results.dislikeCount[i])
#  you[results.v_id[i]]['likeCount'].append(results.likeCount[i])
#  you[results.v_id[i]]['favoriteCount'].append(results.favoriteCount[i])
#  you[results.v_id[i]]['Comment']=comment
