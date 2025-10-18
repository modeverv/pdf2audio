import PyPDF2
import aiohttp
import asyncio
import os
import time
import struct
from typing import List, Tuple, Optional


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
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            full_text = ""
            for page in pdf_reader.pages:
                full_text += page.extract_text()
            
            sentences = [s.strip() + '。' for s in full_text.split('。') if s.strip()]
    
    except FileNotFoundError:
        print(f"エラー: ファイル '{pdf_path}' が見つかりません。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    
    return sentences


async def generate_audio_to_memory_voicevox(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    index: int,
    sentence: str,
    voicevox_url: str,
    speaker_id: int,
    progress_counter: dict
) -> Tuple[bool, int, Optional[bytes], Optional[str]]:
    """
    1つの文章をVoiceVox APIで音声データとしてメモリに生成する（非同期版）
    
    Args:
        session: aiohttp ClientSession
        semaphore: 同時接続数を制限するセマフォ
        index: 文章のインデックス
        sentence: 変換する文章
        voicevox_url: VoiceVoxサーバーのURL
        speaker_id: 話者ID
        progress_counter: 進捗カウンター（参照渡し）
    
    Returns:
        tuple: (success, index, wav_bytes, error_message)
    """
    async with semaphore:
        try:
            # Step 1: audio_query を生成
            async with session.post(
                f"{voicevox_url}/audio_query",
                params={"text": sentence, "speaker": speaker_id},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                response.raise_for_status()
                query_data = await response.json()
            
            # Step 2: 音声合成を実行してWAVバイナリを取得
            async with session.post(
                f"{voicevox_url}/synthesis",
                params={"speaker": speaker_id},
                json=query_data,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                response.raise_for_status()
                wav_bytes = await response.read()
            
            # 進捗更新
            progress_counter['completed'] += 1
            if progress_counter['completed'] % 100 == 0:
                print(f"✓ [{progress_counter['completed']}/{progress_counter['total']}] 生成完了")
            
            return (True, index, wav_bytes, None)
        
        except asyncio.TimeoutError:
            progress_counter['completed'] += 1
            progress_counter['failed'] += 1
            return (False, index, None, "Timeout")
        except aiohttp.ClientError as e:
            progress_counter['completed'] += 1
            progress_counter['failed'] += 1
            return (False, index, None, f"HTTP Error: {str(e)}")
        except Exception as e:
            progress_counter['completed'] += 1
            progress_counter['failed'] += 1
            return (False, index, None, str(e))


async def convert_to_audio_parallel_async_voicevox(
    sentences: List[str],
    voicevox_url: str = "http://127.0.0.1:50021",
    speaker_id: int = 1,
    max_concurrent: int = 100
) -> List[bytes]:
    """
    文章リストをVoiceVox APIで音声データに非同期並列変換
    投げ切れるだけ全部投げて、完了次第処理する
    
    Args:
        sentences: 文章のリスト
        voicevox_url: VoiceVoxサーバーのURL
        speaker_id: 話者ID
        max_concurrent: 最大同時接続数
    
    Returns:
        list: WAVファイルのバイト列リスト（順序保持）
    """
    print(f"\n音声データの生成を開始します（全{len(sentences)}ファイル）...")
    print(f"VoiceVoxサーバー: {voicevox_url}")
    print(f"話者ID: {speaker_id}")
    print(f"最大同時接続数: {max_concurrent}")
    print(f"方式: asyncio完全非同期（すべてのタスクを一度に投入）\n")
    
    # 進捗カウンター
    progress_counter = {
        'completed': 0,
        'failed': 0,
        'total': len(sentences)
    }
    
    # セマフォで同時接続数を制限
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # 結果を格納するリスト（インデックス順を保持）
    wav_bytes_list = [None] * len(sentences)
    
    # ClientSessionを使い回す（接続プーリング）
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=None)  # タスクごとにタイムアウト設定
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 全タスクを一度に作成して投入
        tasks = [
            generate_audio_to_memory_voicevox(
                session, semaphore, i, sentence, voicevox_url, speaker_id, progress_counter
            )
            for i, sentence in enumerate(sentences)
        ]
        
        # すべてのタスクを並列実行（投げ切り）
        print(f"全{len(tasks)}タスクを投入しました。処理中...\n")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 結果を処理
        for result in results:
            if isinstance(result, Exception):
                print(f"✗ 予期しないエラー: {result}")
                continue
            
            success, index, wav_bytes, error_msg = result
            if success:
                wav_bytes_list[index] = wav_bytes
            else:
                print(f"✗ エラー: 文章 {index} - {error_msg}")
    
    print(f"\n音声データの生成が完了しました。")
    print(f"成功: {len(sentences) - progress_counter['failed']}ファイル")
    print(f"失敗: {progress_counter['failed']}ファイル")
    
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
        
        first_wav = wav_bytes_list[0]
        
        if first_wav[:4] != b'RIFF':
            raise ValueError("最初のファイルが有効なWAVファイルではありません")
        
        fmt_index = first_wav.find(b'fmt ')
        data_index = first_wav.find(b'data')
        
        if fmt_index == -1 or data_index == -1:
            raise ValueError("WAVファイルのフォーマットが不正です")
        
        header_size = data_index + 8
        
        print(f"  ヘッダーサイズ: {header_size}バイト")
        print(f"  連結対象: {len(wav_bytes_list)}ファイル")
        
        header = first_wav[:header_size]
        pcm_data = bytearray()
        
        for i, wav_bytes in enumerate(wav_bytes_list):
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
        
        total_data_size = len(pcm_data)
        total_file_size = header_size - 8 + total_data_size
        
        new_header = bytearray(header)
        new_header[4:8] = struct.pack('<I', total_file_size)
        
        data_size_offset = data_index + 4
        new_header[data_size_offset:data_size_offset+4] = struct.pack('<I', total_data_size)
        
        print(f"\n最終ファイルを書き出し中...")
        write_start = time.time()
        
        with open(output_filename, 'wb') as f:
            f.write(new_header)
            f.write(pcm_data)
        
        write_time = time.time() - write_start
        total_time = time.time() - start_time
        
        sample_rate = 24000
        bytes_per_sample = 2
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


async def main():
    """メイン処理"""
    # PDFファイルのパスを指定
    pdf_file = "sample.pdf"
    
    # VoiceVox設定
    VOICEVOX_URL = "http://127.0.0.1:50021"
    SPEAKER_ID = 1  # 1=ずんだもん（ノーマル）
    MAX_CONCURRENT = 100  # 同時接続数（必要に応じて調整）
    
    # 文章を抽出
    print("PDFから文章を抽出中...")
    sentences = extract_sentences_from_pdf(pdf_file)
    
    if sentences:
        print(f"\n抽出された文章数: {len(sentences)}")
        
        for i, sentence in enumerate(sentences[:5]):
            print(f"{i}. {sentence}")
        
        if len(sentences) > 5:
            print(f"... (他 {len(sentences) - 5} 文)")
        
        # 処理時間を計測
        start_time = time.time()
        
        # 音声データをバイナリとして非同期並列生成
        wav_bytes_list = await convert_to_audio_parallel_async_voicevox(
            sentences,
            voicevox_url=VOICEVOX_URL,
            speaker_id=SPEAKER_ID,
            max_concurrent=MAX_CONCURRENT
        )
        
        generation_time = time.time() - start_time
        
        if wav_bytes_list:
            output_filename = f"{pdf_file}_voicevox.wav"
            
            # バイナリレベルで超高速連結
            concatenate_wav_binary(wav_bytes_list, output_filename)
            
            total_time = time.time() - start_time
            
            print(f"\n=== パフォーマンス ===")
            print(f"音声生成時間: {generation_time:.2f}秒 ({generation_time/60:.2f}分)")
            print(f"総処理時間: {total_time:.2f}秒 ({total_time/60:.2f}分)")
            print(f"1文あたり: {generation_time/len(sentences):.3f}秒")
            print(f"\n全ての処理が完了しました！")
            print(f"最終出力ファイル: {os.path.abspath(output_filename)}")
    else:
        print("抽出された文章がありません。")


if __name__ == "__main__":
    # Python 3.7以降
    asyncio.run(main())