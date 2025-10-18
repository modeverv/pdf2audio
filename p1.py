import PyPDF2

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


# 使用例
if __name__ == "__main__":
    # PDFファイルのパスを指定
    pdf_file = "sample.pdf"
    
    # 文章を抽出
    result = extract_sentences_from_pdf(pdf_file)
    
    # 結果を表示
    print(f"抽出された文章数: {len(result)}\n")
    for i, sentence in enumerate(result, 1):
        print(f"{i}. {sentence}")