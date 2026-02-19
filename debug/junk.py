import os

def generate_test_trash():
    # 1. Unsupported extension
    with open("skip_unsupported.cfg", "w") as f:
        f.write("skip_me = True")
        
    # 2. Corrupted Image (Bad header)
    with open("skip_corrupt_img.png", "w") as f:
        f.write("I am a teapot, not a PNG")
        
    # 3. Corrupted Video
    with open("skip_broken_video.mp4", "w") as f:
        f.write("010101010101") # Just garbage bytes

    print("Test files generated. Run your scan now!")

generate_test_trash()