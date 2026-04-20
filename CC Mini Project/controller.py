from flask import Flask, request, send_file, render_template
import os
import subprocess
import re
from cryptography.fernet import Fernet

app = Flask(__name__)

HDFS_PATH = "/user/hadoop/"

# Ensure HDFS directory exists
subprocess.run(f'hdfs dfs -mkdir -p "{HDFS_PATH}"', shell=True)

# Load encryption key
with open("key.key", "rb") as f:
    key = f.read()

cipher = Fernet(key)


# 🔐 Encrypt
def encrypt(data):
    return cipher.encrypt(data)


# 🔓 Decrypt
def decrypt(data):
    return cipher.decrypt(data)


# 📦 Split into blocks (1MB)
def split_data(data, size=1024*1024):
    return [data[i:i+size] for i in range(0, len(data), size)]


# 🏠 Home UI
@app.route('/')
def home():
    return render_template('index.html')


# 📤 Upload file
@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']

    # sanitize filename (remove spaces)
    filename = re.sub(r'\s+', '_', file.filename)

    data = file.read()
    encrypted = encrypt(data)

    blocks = split_data(encrypted)

    for i, block in enumerate(blocks):
        block_name = f"{filename}_part{i}"

        with open(block_name, 'wb') as f:
            f.write(block)

        os.system(f'hdfs dfs -put "{block_name}" "{HDFS_PATH}"')
        os.remove(block_name)

    return f"Uploaded: {filename}"


# 📄 List files (clean names)
@app.route('/list')
def list_files():
    output = subprocess.getoutput(f'hdfs dfs -ls "{HDFS_PATH}"')
    lines = output.split("\n")

    files = set()

    for line in lines:
        if "_part" in line:
            name = line.split("/")[-1]
            original = name.split("_part")[0]
            files.add(original)

    return "\n".join(sorted(files))


# 📥 Download file (merge + decrypt)
@app.route('/download/<filename>')
def download(filename):

    output = subprocess.getoutput(f'hdfs dfs -ls "{HDFS_PATH}"')
    lines = output.split("\n")

    blocks = []

    for line in lines:
        if filename in line:
            parts = line.split()
            blocks.append(parts[-1])

    # ✅ Correct sorting (important fix)
    blocks.sort(key=lambda x: int(x.split("_part")[-1]))

    data = b''

    for block in blocks:
        os.system(f'hdfs dfs -get "{block}" .')
        local_file = block.split("/")[-1]

        with open(local_file, 'rb') as f:
            data += f.read()

        os.remove(local_file)

    try:
        decrypted = decrypt(data)
    except:
        return "Decryption failed (possible corruption or key mismatch)"

    output_file = "download_" + filename

    with open(output_file, 'wb') as f:
        f.write(decrypted)

    return send_file(output_file, as_attachment=True)


# 🗑 Delete file
@app.route('/delete/<filename>')
def delete(filename):

    output = subprocess.getoutput(f'hdfs dfs -ls "{HDFS_PATH}"')
    lines = output.split("\n")

    for line in lines:
        if filename in line:
            filepath = line.split()[-1]
            os.system(f'hdfs dfs -rm "{filepath}"')

    return f"Deleted: {filename}"


# 🚀 Run server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
