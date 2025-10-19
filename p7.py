import pdfplumber
import subprocess
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import time
import struct
import sys

def extract_sentences_from_pdf(pdf_path):
    """
    PDFファイルから文章を抽出し、句点で分割してリストに格納する
    
    Args:
        pdf_path (str): PDFファイルのパス
    
    Returns:
        list: 句点で分割された文章のリスト
    """
    sentences = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
               full_text += page.extract_text()
               
            # 句点（。）で分割
            sentences = [s.strip() + '。' for s in full_text.split('。') if s.strip()]

            with open(pdf_path + ".txt", 'w', encoding='utf-8') as file:
                file.write("\n".join(sentences))

    except FileNotFoundError:
        print(f"エラー: ファイル '{pdf_path}' が見つかりません。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    
    return sentences


def generate_audio_to_memory(args):
    """
    1つの文章を音声データとしてメモリに生成する（並列処理用）
    名前付きパイプ(FIFO)を使用してメモリ上でデータをやり取り
    
    Args:
        args (tuple): (index, sentence, voice)
    
    Returns:
        tuple: (success, index, wav_bytes, error_message)
    """
    i, sentence, voice = args
    
    import tempfile
    
    # 一時ディレクトリに名前付きパイプを作成
    with tempfile.TemporaryDirectory() as tmpdir:
        fifo_path = os.path.join(tmpdir, f'speech_{i}.fifo')
        
        try:
            # FIFOを作成（メモリ上でのパイプ通信）
            os.mkfifo(fifo_path)
            
            # sayコマンドをバックグラウンドで起動
            # --data-formatを削除し、デフォルトのAIFFフォーマットを使用
            process = subprocess.Popen(
                ["say", "-r", "500", "-v", voice, "-o", fifo_path, sentence],
                stderr=subprocess.PIPE
            )
            
            # FIFOからデータを読み込み（メモリ上の通信）
            with open(fifo_path, 'rb') as fifo:
                wav_bytes = fifo.read()
            
            # プロセスの終了を待つ
            return_code = process.wait(timeout=30)
            
            if return_code != 0:
                stderr = process.stderr.read().decode('utf-8', errors='ignore')
                return (False, i, None, f"sayコマンドエラー: {stderr}")
            
            return (True, i, wav_bytes, None)
        
        except subprocess.TimeoutExpired:
            process.kill()
            return (False, i, None, "タイムアウト: 30秒以内に完了しませんでした")
        except Exception as e:
            if process.poll() is None:
                process.kill()
            return (False, i, None, str(e))


def convert_to_audio_parallel_memory(sentences, voice="Kyoko", max_workers=None):
    """
    文章リストをsayコマンドで音声データに並列変換（完全メモリ処理）
    
    Args:
        sentences (list): 文章のリスト
        voice (str): 使用する音声（macOSの日本語音声: "Kyoko" など）
        max_workers (int): 並列実行するワーカー数（Noneの場合はCPUコア数）
    
    Returns:
        list: WAVファイルのバイト列リスト（順序保持）
    """
    # ワーカー数の決定
    if max_workers is None:
        max_workers = multiprocessing.cpu_count() 
    
    print(f"\n音声データの生成を開始します（全{len(sentences)}ファイル）...")
    print(f"並列処理: {max_workers}ワーカーを使用")
    print(f"方式: 名前付きパイプ(FIFO)でメモリ上のデータ転送（ディスクI/O不要）\n")
    
    # 並列処理用の引数リストを作成
    tasks = [(i, sentence, voice) for i, sentence in enumerate(sentences)]
    
    wav_bytes_list = [None] * len(sentences)  # インデックス順を保持するためのリスト
    completed = 0
    failed = 0
    
    # ProcessPoolExecutorで並列実行
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 全タスクを投入
        future_to_index = {executor.submit(generate_audio_to_memory, task): task[0] 
                          for task in tasks}
        
        # 完了したタスクから順次処理
        for future in as_completed(future_to_index):
            success, index, wav_bytes, error_msg = future.result()
            completed += 1
            
            if success:
                wav_bytes_list[index] = wav_bytes
                if completed % 100 == 0:  # 100件ごとに表示
                    print(f"✓ [{completed}/{len(sentences)}] 生成完了")
            else:
                failed += 1
                print(f"✗ [{completed}/{len(sentences)}] エラー: 文章 {index} - {error_msg}")
    
    print(f"\n音声データの生成が完了しました。")
    print(f"成功: {len(sentences) - failed}ファイル, 失敗: {failed}ファイル")
    
    # Noneを除外（失敗したファイル）
    wav_bytes_list = [data for data in wav_bytes_list if data is not None]
    
    return wav_bytes_list


def concatenate_wav_binary(wav_bytes_list, output_filename):
    """
    AIFF/WAVファイルのバイナリデータを直接連結（超高速）
    
    Args:
        wav_bytes_list (list): AIFF/WAVファイルのバイト列リスト
        output_filename (str): 出力ファイル名
    """
    if not wav_bytes_list:
        print("連結するデータがありません。")
        return
    
    print(f"\nAIFF/WAVファイルをバイナリレベルで連結中...")
    print(f"方式: ヘッダー除去 + バイト列連結（超高速）")
    
    try:
        start_time = time.time()
        
        # 最初のファイルからヘッダー情報を取得
        first_audio = wav_bytes_list[0]
        
        # フォーマット判定（RIFF=WAV, FORM=AIFF）
        is_wav = first_audio[:4] == b'RIFF'
        is_aiff = first_audio[:4] == b'FORM'
        
        if not (is_wav or is_aiff):
            raise ValueError("最初のファイルが有効なWAV/AIFFファイルではありません")
        
        file_format = "WAV" if is_wav else "AIFF"
        print(f"  検出されたフォーマット: {file_format}")
        
        # データチャンクを探す
        if is_wav:
            data_marker = b'data'
        else:  # AIFF
            data_marker = b'SSND'
        
        data_index = first_audio.find(data_marker)
        
        if data_index == -1:
            raise ValueError(f"{file_format}ファイルのデータチャンクが見つかりません")
        
        # AIFFの場合、SSNDチャンクには8バイトのヘッダー情報がある
        if is_aiff:
            # SSND + サイズ(4) + offset(4) + blockSize(4) = 16バイト
            header_size = data_index + 16
        else:
            # data + サイズ(4) = 8バイト
            header_size = data_index + 8
        
        print(f"  ヘッダーサイズ: {header_size}バイト")
        print(f"  連結対象: {len(wav_bytes_list)}ファイル")
        
        # 最初のファイルのヘッダーを保持
        header = first_audio[:header_size]
        
        # 全てのPCMデータを連結
        pcm_data = bytearray()
        
        for i, audio_bytes in enumerate(wav_bytes_list):
            # 各ファイルのデータチャンクを探す
            file_data_index = audio_bytes.find(data_marker)
            if file_data_index == -1:
                print(f"  警告: ファイル{i}のデータチャンクが見つかりません。スキップします。")
                continue
            
            if is_aiff:
                file_header_size = file_data_index + 16
            else:
                file_header_size = file_data_index + 8
                
            pcm_data.extend(audio_bytes[file_header_size:])
            
            if (i + 1) % 500 == 0:
                print(f"  {i + 1}/{len(wav_bytes_list)} ファイル処理済み...")
        
        concat_time = time.time() - start_time
        print(f"  バイト列連結完了: {concat_time:.2f}秒")
        
        # 新しいヘッダーを作成（ファイルサイズを更新）
        total_data_size = len(pcm_data)
        
        # ヘッダーを更新
        new_header = bytearray(header)
        
        if is_wav:
            # WAVの場合
            total_file_size = header_size - 8 + total_data_size
            # RIFFチャンクサイズを更新（4〜7バイト目）
            new_header[4:8] = struct.pack('<I', total_file_size)
            # dataチャンクサイズを更新
            data_size_offset = data_index + 4
            new_header[data_size_offset:data_size_offset+4] = struct.pack('<I', total_data_size)
        else:
            # AIFFの場合
            total_file_size = header_size - 8 + total_data_size
            # FORMチャンクサイズを更新（4〜7バイト目、ビッグエンディアン）
            new_header[4:8] = struct.pack('>I', total_file_size)
            # SSNDチャンクサイズを更新（ビッグエンディアン）
            ssnd_size_offset = data_index + 4
            new_header[ssnd_size_offset:ssnd_size_offset+4] = struct.pack('>I', total_data_size + 8)
        
        # ファイルに書き出し
        print(f"\n最終ファイルを書き出し中...")
        write_start = time.time()
        
        # 出力ファイル名の拡張子を適切に設定
        if is_aiff and not output_filename.endswith('.aiff'):
            output_filename = output_filename.replace('.wav', '.aiff')
        
        with open(output_filename, 'wb') as f:
            f.write(new_header)
            f.write(pcm_data)
        
        write_time = time.time() - write_start
        total_time = time.time() - start_time
        
        # 再生時間を計算（フォーマットに応じて）
        # デフォルトAIFF: 16bit @ 22050Hz = 2バイト/サンプル
        bytes_per_sample = 2
        sample_rate = 22050
        duration_seconds = total_data_size / (bytes_per_sample * sample_rate)
        
        print(f"✓ 連結完了: {output_filename}")
        print(f"  ファイルサイズ: {os.path.getsize(output_filename) / (1024*1024):.2f} MB")
        print(f"  PCMデータサイズ: {total_data_size / (1024*1024):.2f} MB")
        print(f"  再生時間: {duration_seconds / 60:.2f} 分")
        print(f"\n  処理時間内訳:")
        print(f"    バイト列連結: {concat_time:.2f}秒")
        print(f"    ファイル書き出し: {write_time:.2f}秒")
        print(f"    合計: {total_time:.2f}秒")
        
    except Exception as e:
        print(f"✗ 連結エラー: {e}")
        import traceback
        traceback.print_exc()
        traceback.print_exc()

# 使用例
if __name__ == "__main__":
    # PDFファイルのパスを指定
    if len(sys.argv) < 2:
        print("使用方法: python p4.py <PDFファイル名>")
        sys.exit(1)
    
    pdf_file = sys.argv[1]
    
    # 文章を抽出
    print("PDFから文章を抽出中...")
    sentences = extract_sentences_from_pdf(pdf_file)
    
    if sentences:
        # 結果を表示
        print(f"\n抽出された文章数: {len(sentences)}")
        print(f"使用可能なCPUコア数: {multiprocessing.cpu_count()}")
        
        for i, sentence in enumerate(sentences[:5]):  # 最初の5文だけ表示
            print(f"{i}. {sentence}")
        
        if len(sentences) > 5:
            print(f"... (他 {len(sentences) - 5} 文)")
        
        # 想定処理時間の計算
        estimated_time_serial = len(sentences) * 5  # 1文5秒と仮定
        estimated_time_parallel = estimated_time_serial / multiprocessing.cpu_count()
        print(f"\n想定処理時間（逐次処理）: 約{estimated_time_serial / 60:.1f}分")
        print(f"想定処理時間（並列処理）: 約{estimated_time_parallel / 60:.1f}分")
        
        # 処理時間を計測
        start_time = time.time()
        
        # 音声データをパイプ経由で並列生成（ファイルI/O完全省略）
        wav_bytes_list = convert_to_audio_parallel_memory(sentences, max_workers=None)
        
        generation_time = time.time() - start_time
        
        if wav_bytes_list:
            output_filename = f"{pdf_file}.aiff"  # AIFFフォーマットで出力
            
            # バイナリレベルで超高速連結
            concatenate_wav_binary(wav_bytes_list, output_filename)
            
            total_time = time.time() - start_time
            
            print(f"\n=== パフォーマンス ===")
            print(f"音声生成時間: {generation_time:.2f}秒 ({generation_time/60:.2f}分)")
            print(f"総処理時間: {total_time:.2f}秒 ({total_time/60:.2f}分)")
            print(f"1文あたり: {generation_time/len(sentences):.3f}秒")
            print(f"実効スピードアップ: 約{estimated_time_serial / generation_time:.1f}倍")
            print(f"\n全ての処理が完了しました！")
            print(f"最終出力ファイル: {os.path.abspath(output_filename)}")
    else:
        print("抽出された文章がありません。")