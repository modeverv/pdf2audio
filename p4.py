import PyPDF2
import subprocess
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import tempfile
import time
import struct

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
        # PDFファイルを開く
        with open(pdf_path, 'rb') as file:
            # PDFリーダーオブジェクトを作成
            pdf_reader = PyPDF2.PdfReader(file)
            
            # 全ページからテキストを抽出
            full_text = ""
            for page in pdf_reader.pages:
                full_text += page.extract_text()
            
            # 句点（。）で分割
            sentences = [s.strip() + '。' for s in full_text.split('。') if s.strip()]
    
    except FileNotFoundError:
        print(f"エラー: ファイル '{pdf_path}' が見つかりません。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    
    return sentences


def generate_audio_to_memory(args):
    """
    1つの文章を音声データとしてメモリに生成する（並列処理用）
    WAVファイルをバイナリデータとして返す
    
    Args:
        args (tuple): (index, sentence, voice)
    
    Returns:
        tuple: (success, index, wav_bytes, error_message)
    """
    i, sentence, voice = args
    
    # 一時ファイルを作成
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp_file:
        tmp_path = tmp_file.name
    
    try:
        # sayコマンドで一時ファイルに出力
        subprocess.run(
            ["say", "-r", "500", "-v", voice, "-o", tmp_path, "--data-format=LEF32@22050", sentence],
            check=True,
            capture_output=True
        )
        
        # WAVファイルをバイナリとして読み込み
        with open(tmp_path, 'rb') as f:
            wav_bytes = f.read()
        
        # 一時ファイルを削除
        os.unlink(tmp_path)
        
        return (True, i, wav_bytes, None)
    
    except subprocess.CalledProcessError as e:
        # エラー時も一時ファイルを削除
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return (False, i, None, str(e))
    except Exception as e:
        # エラー時も一時ファイルを削除
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return (False, i, None, str(e))


def convert_to_audio_parallel_memory(sentences, voice="Kyoko", max_workers=None):
    """
    文章リストをsayコマンドで音声データに並列変換（バイナリで保持）
    
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
    print(f"方式: バイナリデータとしてメモリに保持\n")
    
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
    WAVファイルのバイナリデータを直接連結（超高速）
    
    Args:
        wav_bytes_list (list): WAVファイルのバイト列リスト
        output_filename (str): 出力ファイル名
    """
    if not wav_bytes_list:
        print("連結するデータがありません。")
        return
    
    print(f"\nWAVファイルをバイナリレベルで連結中...")
    print(f"方式: ヘッダー除去 + バイト列連結（超高速）")
    
    try:
        start_time = time.time()
        
        # 最初のファイルからヘッダー情報を取得
        first_wav = wav_bytes_list[0]
        
        # WAVヘッダーを解析（RIFF形式）
        # 参考: http://soundfile.sapp.org/doc/WaveFormat/
        
        # RIFFヘッダーを確認
        if first_wav[:4] != b'RIFF':
            raise ValueError("最初のファイルが有効なWAVファイルではありません")
        
        # fmtチャンクを探す（通常は12バイト目から）
        fmt_index = first_wav.find(b'fmt ')
        data_index = first_wav.find(b'data')
        
        if fmt_index == -1 or data_index == -1:
            raise ValueError("WAVファイルのフォーマットが不正です")
        
        # dataチャンクのヘッダーサイズを取得（通常44バイト）
        header_size = data_index + 8  # 'data' + サイズ(4バイト) = 8バイト
        
        print(f"  ヘッダーサイズ: {header_size}バイト")
        print(f"  連結対象: {len(wav_bytes_list)}ファイル")
        
        # 最初のファイルのヘッダーを保持
        header = first_wav[:header_size]
        
        # 全てのPCMデータを連結
        pcm_data = bytearray()
        
        for i, wav_bytes in enumerate(wav_bytes_list):
            # 各ファイルのdataチャンクを探す
            file_data_index = wav_bytes.find(b'data')
            if file_data_index == -1:
                print(f"  警告: ファイル{i}のdataチャンクが見つかりません。スキップします。")
                continue
            
            file_header_size = file_data_index + 8
            pcm_data.extend(wav_bytes[file_header_size:])
            
            if (i + 1) % 500 == 0:
                print(f"  {i + 1}/{len(wav_bytes_list)} ファイル処理済み...")
        
        concat_time = time.time() - start_time
        print(f"  バイト列連結完了: {concat_time:.2f}秒")
        
        # 新しいヘッダーを作成（ファイルサイズを更新）
        total_data_size = len(pcm_data)
        total_file_size = header_size - 8 + total_data_size  # RIFF識別子とサイズフィールドを除く
        
        # ヘッダーを更新
        new_header = bytearray(header)
        
        # RIFFチャンクサイズを更新（4〜7バイト目）
        new_header[4:8] = struct.pack('<I', total_file_size)
        
        # dataチャンクサイズを更新（dataチャンク位置+4バイト目から4バイト）
        data_size_offset = data_index + 4
        new_header[data_size_offset:data_size_offset+4] = struct.pack('<I', total_data_size)
        
        # ファイルに書き出し
        print(f"\n最終ファイルを書き出し中...")
        write_start = time.time()
        
        with open(output_filename, 'wb') as f:
            f.write(new_header)
            f.write(pcm_data)
        
        write_time = time.time() - write_start
        total_time = time.time() - start_time
        
        # 再生時間を計算（32bit float @ 22050Hz = 4バイト/サンプル）
        duration_seconds = total_data_size / (4 * 22050)
        
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


# 使用例
if __name__ == "__main__":
    # PDFファイルのパスを指定
    pdf_file = "sample.pdf"
    
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
        
        # 音声データをバイナリとして並列生成
        wav_bytes_list = convert_to_audio_parallel_memory(sentences, max_workers=None)
        
        generation_time = time.time() - start_time
        
        if wav_bytes_list:
            output_filename = f"{pdf_file}.wav"
            
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
