import PyPDF2
import subprocess
import os
from pydub import AudioSegment
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import tempfile
import time

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
    一時ファイルを使用するが、即座にメモリに読み込んで削除
    
    Args:
        args (tuple): (index, sentence, voice)
    
    Returns:
        tuple: (success, index, audio_segment, error_message)
    """
    i, sentence, voice = args
    
    # 一時ファイルを作成
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp_file:
        tmp_path = tmp_file.name
    
    try:
        # sayコマンドで一時ファイルに出力
        subprocess.run(
            ["say", "-v", voice, "-o", tmp_path, "--data-format=LEF32@22050", sentence],
            check=True,
            capture_output=True
        )
        
        # 一時ファイルをメモリに読み込み
        audio_segment = AudioSegment.from_wav(tmp_path)
        
        # 一時ファイルを削除
        os.unlink(tmp_path)
        
        return (True, i, audio_segment, None)
    
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
    文章リストをsayコマンドで音声データに並列変換（メモリ上で処理）
    
    Args:
        sentences (list): 文章のリスト
        voice (str): 使用する音声（macOSの日本語音声: "Kyoko" など）
        max_workers (int): 並列実行するワーカー数（Noneの場合はCPUコア数）
    
    Returns:
        list: AudioSegmentオブジェクトのリスト（順序保持）
    """
    # ワーカー数の決定
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
    
    print(f"\n音声データの生成を開始します（全{len(sentences)}ファイル）...")
    print(f"並列処理: {max_workers}ワーカーを使用")
    print(f"方式: 一時ファイル経由でメモリに即座に読み込み\n")
    
    # 並列処理用の引数リストを作成
    tasks = [(i, sentence, voice) for i, sentence in enumerate(sentences)]
    
    audio_segments = [None] * len(sentences)  # インデックス順を保持するためのリスト
    completed = 0
    failed = 0
    
    # ProcessPoolExecutorで並列実行
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 全タスクを投入
        future_to_index = {executor.submit(generate_audio_to_memory, task): task[0] 
                          for task in tasks}
        
        # 完了したタスクから順次処理
        for future in as_completed(future_to_index):
            success, index, audio_segment, error_msg = future.result()
            completed += 1
            
            if success:
                audio_segments[index] = audio_segment
                print(f"✓ [{completed}/{len(sentences)}] 生成完了: 文章 {index}")
            else:
                failed += 1
                print(f"✗ [{completed}/{len(sentences)}] エラー: 文章 {index} - {error_msg}")
    
    print(f"\n音声データの生成が完了しました。")
    print(f"成功: {len(sentences) - failed}ファイル, 失敗: {failed}ファイル")
    
    # Noneを除外（失敗したファイル）
    audio_segments = [seg for seg in audio_segments if seg is not None]
    
    return audio_segments


def save_individual_files(audio_segments, output_dir="out"):
    """
    メモリ上の音声データをファイルとして保存（オプション）
    
    Args:
        audio_segments (list): AudioSegmentオブジェクトのリスト
        output_dir (str): 出力ディレクトリ
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"\n個別ファイルを保存中...")
    
    for i, segment in enumerate(audio_segments):
        output_file = os.path.join(output_dir, f"{i}.wav")
        segment.export(output_file, format="wav")
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(audio_segments)} ファイル保存済み...")
    
    print(f"✓ 個別ファイルの保存が完了しました: {output_dir}/")


def concatenate_audio_segments(audio_segments, output_filename):
    """
    メモリ上の音声データを連結して1つのファイルにする
    
    Args:
        audio_segments (list): AudioSegmentオブジェクトのリスト
        output_filename (str): 出力ファイル名
    """
    if not audio_segments:
        print("連結するデータがありません。")
        return
    
    print(f"\n音声データを連結中...")
    
    try:
        # 最初のセグメント
        combined = audio_segments[0]
        
        # 残りのセグメントを順番に連結
        for i, segment in enumerate(audio_segments[1:], 1):
            combined += segment
            if (i) % 100 == 0:  # 100ファイルごとに進捗表示
                print(f"  {i}/{len(audio_segments)-1} セグメント連結済み...")
        
        # 連結したファイルを保存
        print(f"\n最終ファイルを書き出し中...")
        combined.export(output_filename, format="wav")
        
        print(f"✓ 連結完了: {output_filename}")
        print(f"  ファイルサイズ: {os.path.getsize(output_filename) / (1024*1024):.2f} MB")
        print(f"  再生時間: {len(combined) / 1000 / 60:.2f} 分")
    
    except Exception as e:
        print(f"✗ 連結エラー: {e}")


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
        
        # 音声データをメモリ上で並列生成
        audio_segments = convert_to_audio_parallel_memory(sentences, max_workers=None)
        
        generation_time = time.time() - start_time
        
        if audio_segments:
            # オプション: 個別ファイルとして保存する場合
            # save_individual_files(audio_segments, output_dir="out")
            
            # 音声データを連結して保存
            output_filename = f"{pdf_file}.wav"
            concatenate_audio_segments(audio_segments, output_filename)
            
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