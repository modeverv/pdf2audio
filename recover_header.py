import struct
import os

def reconstruct_wav_header_rf64(input_file, output_file, channels=1, sample_rate=22050, bits_per_sample=32, audio_format=3, skip_bytes=0x1020):
    """
    大きなWAVファイル(>4GB)のヘッダをRF64形式で再構築する
    
    Parameters:
    -----------
    input_file : str
        入力ファイル
    output_file : str
        出力ファイル
    channels : int
        チャンネル数 (1=モノラル, 2=ステレオ)
    sample_rate : int
        サンプリングレート
    bits_per_sample : int
        ビット深度 (32 for float)
    audio_format : int
        オーディオフォーマット (1=PCM整数, 3=IEEE float)
    skip_bytes : int
        スキップする既存ヘッダのバイト数 (0x1020 = 4128バイト)
    """
    
    # 元のファイルサイズを取得
    file_size = os.path.getsize(input_file)
    
    print(f"ファイルサイズ: {file_size:,} bytes ({file_size / (1024**3):.2f} GB)")
    
    # PCMデータのサイズを計算
    pcm_data_size = file_size - skip_bytes
    
    print(f"スキップするヘッダサイズ: {skip_bytes} bytes (0x{skip_bytes:X})")
    print(f"PCMデータサイズ: {pcm_data_size:,} bytes ({pcm_data_size / (1024**3):.2f} GB)")
    print(f"オーディオ形式: {'IEEE Float' if audio_format == 3 else 'PCM'}")
    print(f"チャンネル数: {channels}")
    print(f"サンプリングレート: {sample_rate} Hz")
    print(f"ビット深度: {bits_per_sample} bit")
    
    # バイト/サンプル、バイト/秒を計算
    byte_rate = sample_rate * channels * bits_per_sample // 8
    block_align = channels * bits_per_sample // 8
    
    # RF64ヘッダを構築
    with open(output_file, 'wb') as out_f:
        # RF64ヘッダ
        out_f.write(b'RF64')
        out_f.write(struct.pack('<I', 0xFFFFFFFF))
        out_f.write(b'WAVE')
        
        # ds64チャンク (64bit拡張サイズ情報)
        out_f.write(b'ds64')
        out_f.write(struct.pack('<I', 28))
        out_f.write(struct.pack('<Q', pcm_data_size + 60))
        out_f.write(struct.pack('<Q', pcm_data_size))
        out_f.write(struct.pack('<Q', 0))
        out_f.write(struct.pack('<I', 0))
        
        # fmtチャンク
        out_f.write(b'fmt ')
        out_f.write(struct.pack('<I', 16))
        out_f.write(struct.pack('<H', audio_format))  # 3 = IEEE float
        out_f.write(struct.pack('<H', channels))
        out_f.write(struct.pack('<I', sample_rate))
        out_f.write(struct.pack('<I', byte_rate))
        out_f.write(struct.pack('<H', block_align))
        out_f.write(struct.pack('<H', bits_per_sample))
        
        # dataチャンク
        out_f.write(b'data')
        out_f.write(struct.pack('<I', 0xFFFFFFFF))
        
        # PCMデータをコピー
        with open(input_file, 'rb') as in_f:
            in_f.seek(skip_bytes)
            
            chunk_size = 1024 * 1024 * 10  # 10MB
            copied = 0
            
            while True:
                chunk = in_f.read(chunk_size)
                if not chunk:
                    break
                out_f.write(chunk)
                copied += len(chunk)
                
                progress = (copied / pcm_data_size) * 100
                print(f"\r進捗: {progress:.1f}% ({copied:,} / {pcm_data_size:,} bytes)", end='')
    
    print(f"\n\n✅ ヘッダを再構築しました (RF64形式)")
    print(f"出力ファイル: {output_file}")


# 使用例
if __name__ == "__main__":
    reconstruct_wav_header_rf64(
        input_file="pdfs/マイクロサービスアーキテクチャ2版.pdf.wav",
        output_file="pdfs/マイクロサービスアーキテクチャ2版.pdf.wav_correct.wav",
        channels=1,           # モノラル
        sample_rate=22050,    # 22.05kHz
        bits_per_sample=32,   # 32bit
        audio_format=3,       # IEEE float
        skip_bytes=0x1020     # 4128バイト (hexdumpで確認したdataチャンクの開始位置)
    )