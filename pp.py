import subprocess
import time

text = "こんにちは、世界"
iterations = 100

# ファイル経由
start = time.time()
for i in range(iterations):
    subprocess.run(['say', '-o', f'/tmp/test{i}.aiff', text])
    with open(f'/tmp/test{i}.aiff', 'rb') as f:
        data = f.read()
file_time = time.time() - start

# パイプ経由
start = time.time()
for i in range(iterations):
    result = subprocess.run(['say', '-o', '-', '--data-format=LEF32@22050', text], 
                          capture_output=True)
    data = result.stdout
pipe_time = time.time() - start

print(f"ファイル: {file_time:.2f}秒")
print(f"パイプ: {pipe_time:.2f}秒")
print(f"高速化: {(file_time/pipe_time - 1)*100:.1f}%")