# The following code will only execute
# successfully when compression is complete

import kagglehub

def get_data():
# Download latest version
    path = kagglehub.dataset_download("rsrishav/youtube-trending-video-dataset")

    print("Path to dataset files:", path)
    return path