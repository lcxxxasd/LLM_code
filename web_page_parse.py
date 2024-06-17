from trafilatura import fetch_url, extract
url = 'https://baike.baidu.com/item/2024%E5%B9%B4%E5%B7%B4%E9%BB%8E%E5%A5%A5%E8%BF%90%E4%BC%9A/17619118?fr=ge_ala'
# url = 'https://zh.wikipedia.org/wiki/2024%E5%B9%B4%E5%A4%8F%E5%AD%A3%E5%A5%A5%E6%9E%97%E5%8C%B9%E5%85%8B%E8%BF%90%E5%8A%A8%E4%BC%9A'
downloaded = fetch_url(url)
# print(downloaded)
result = extract(downloaded, output_format="json")
print(result)