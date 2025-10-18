import PyPDF2
import subprocess
import os

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


def convert_to_audio(sentences, output_dir="out", voice="Kyoko"):
    """
    文章リストをsayコマンドでwavファイルに変換
    
    Args:
        sentences (list): 文章のリスト
        output_dir (str): 出力ディレクトリ
        voice (str): 使用する音声（macOSの日本語音声: "Kyoko" など）
    """
    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"ディレクトリ '{output_dir}' を作成しました。")
    
    print(f"\n音声ファイルの生成を開始します（全{len(sentences)}ファイル）...\n")
    
    for i, sentence in enumerate(sentences):
        # 出力ファイル名
        output_file = os.path.join(output_dir, f"{i}.wav")
        
        try:
            # sayコマンドを実行
            subprocess.run(
                ["say", "-v", voice, "-o", output_file, "--data-format=LEF32@22050", sentence],
                check=True
            )
            print(f"✓ 生成完了: {output_file}")
        
        except subprocess.CalledProcessError as e:
            print(f"✗ エラー: {output_file} の生成に失敗しました - {e}")
        except Exception as e:
            print(f"✗ 予期しないエラー: {e}")
    
    print(f"\n全ての音声ファイルの生成が完了しました。")
    print(f"出力先: {os.path.abspath(output_dir)}")


# 使用例
if __name__ == "__main__":
    # PDFファイルのパスを指定
    pdf_file = "sample.pdf"
    
    # 文章を抽出
    print("PDFから文章を抽出中...")
    sentences = extract_sentences_from_pdf(pdf_file)
    
    if sentences:
        # 結果を表示
        print(f"\n抽出された文章数: {len(sentences)}\n")
        for i, sentence in enumerate(sentences[:5]):  # 最初の5文だけ表示
            print(f"{i}. {sentence}")
        
        if len(sentences) > 5:
            print(f"... (他 {len(sentences) - 5} 文)")
        
        # 音声ファイルに変換
        convert_to_audio(sentences)
    else:
        print("抽出された文章がありません。")