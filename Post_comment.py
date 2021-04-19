import json
import boto3
import datetime
import pandas as pd
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_colwidth', -1)

def lambda_handler(event, context):
    print(pd.__version__)
    pd.show_versions()

    post_url = 'https://jsonplaceholder.typicode.com/posts'
    comments_url = 'https://jsonplaceholder.typicode.com/comments'
    file_name = 'Posts_Comments.json'
    bucket_name = 'xl-post'
    try:
        step = 0
        posts = pd.read_json(post_url)
        step += 1
        comments = pd.read_json(comments_url)
    except Exception as e:
        print(f'Failed after Step : {step}')
        print(e)
    post_comment = pd.merge(posts, comments, how='outer', left_on='id', right_on='postId')
    post_comment = post_comment.rename(
        columns={'body_x': 'post_body', 'id_x': 'post_id', 'body_y': 'comments_body', 'id_y': 'comments_id',
                 'postId': 'comment_postId'})
    data_frame = post_comment[['userId', 'post_body', 'title', 'comment_postId', 'comments_body', 'email', 'name']]
    Final_data = (data_frame.groupby(['userId', 'post_body', 'title'], as_index=True)
                  .apply(lambda x: x[['comment_postId', 'comments_body', 'email', 'name']].to_dict('r'))
                  .reset_index()
                  .rename(columns={0: 'comments'})
                  .to_json(orient='records'))
    Final_json = json.dumps(json.loads(Final_data), indent=2, sort_keys=False)
    with open("/tmp/" + file_name, "w") as outfile:
        outfile.write(Final_json)
    s3_client = boto3.client('s3')
    response = s3_client.upload_file('/tmp/' + file_name, bucket_name, file_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Post and Comments merged and posted as a JSON to S3 at '+ str(datetime.datetime.now()))
    }
