from flask import Flask, request, jsonify
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from io import BytesIO
from dotenv import load_dotenv
import sqlite3
import os
from uuid import uuid4

load_dotenv()

app = Flask(__name__)

# AWS S3 configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('videos.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        url TEXT NOT NULL
    )
    ''')
    conn.commit()
    conn.close()

init_db()

# Helper function to fetch all videos from database
def fetch_all_videos():
    conn = sqlite3.connect('videos.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, filename, url FROM videos")
    videos = cursor.fetchall()
    conn.close()
    return videos

# Helper function to fetch a single video from database by id
def fetch_video(video_id):
    conn = sqlite3.connect('videos.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, filename, url FROM videos WHERE id=?", (video_id,))
    video = cursor.fetchone()
    conn.close()
    return video

@app.route('/videos', methods=['GET'])
def get_all_videos():
    videos = fetch_all_videos()
    videos_list = []
    for video in videos:
        video_dict = {
            "id": video[0],
            "filename": video[1],
            "url": video[2]
        }
        videos_list.append(video_dict)
    return jsonify({"videos": videos_list}), 200

@app.route('/videos/<int:video_id>', methods=['GET'])
def get_video(video_id):
    video = fetch_video(video_id)
    if video:
        video_dict = {
            "id": video[0],
            "filename": video[1],
            "url": video[2]
        }
        return jsonify({"video": video_dict}), 200
    else:
        return jsonify({"error": "Video not found"}), 404

def delete_video_from_s3(filename):
    try:
        s3_client.delete_object(Bucket=AWS_BUCKET_NAME, Key=filename)
    except ClientError as e:
        return str(e)
    return None

@app.route('/videos/<int:video_id>', methods=['DELETE'])
def delete_video(video_id):
    video = fetch_video(video_id)
    if not video:
        return jsonify({"error": "Video not found"}), 404

    filename = video[1]  # Fetching filename from database record

    # Delete video file from S3
    delete_error = delete_video_from_s3(filename)
    if delete_error:
        return jsonify({"error": f"Failed to delete video from S3: {delete_error}"}), 500

    # Delete video record from SQLite database
    conn = sqlite3.connect('videos.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM videos WHERE id=?", (video_id,))
    conn.commit()
    conn.close()

    return jsonify({"message": "Video deleted successfully"}), 200

def generate_unique_filename(filename):
    unique_filename = str(uuid4()) + '_' + filename  # append UUID to original filename
    return unique_filename

@app.route('/upload-video', methods=['POST'])
def upload_video():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        # Generate a unique filename
        unique_filename = generate_unique_filename(file.filename)

        file_contents = file.read()
        s3_client.upload_fileobj(
            BytesIO(file_contents),
            AWS_BUCKET_NAME,
            unique_filename,
            ExtraArgs={"ContentType": file.content_type}
        )
        file_url = f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{unique_filename}"

        # Save file URL with original filename to SQLite database
        conn = sqlite3.connect('videos.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO videos (filename, url) VALUES (?, ?)", (file.filename, file_url))
        conn.commit()
        conn.close()

        return jsonify({"file_url": file_url}), 200

    except NoCredentialsError:
        return jsonify({"error": "AWS credentials not found"}), 403
    except PartialCredentialsError:
        return jsonify({"error": "Incomplete AWS credentials"}), 403
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)