from flask import Flask, render_template, jsonify, request
import requests
from datetime import datetime, timedelta
import threading
import time
import os
import urllib.parse
import pandas as pd


app = Flask(__name__)

buffers = []
current_pasring = []
keywords = ["신창섭", "창드컵", "c-pop", "창섭", "창pop", "창팝"]
data_refer = {};


chizzBannListID = [
    "3f3255ae1d8d93fcdb7d180ba3bcb102",
    "a20dcbd9d7ef1599b3256bad5e787ccb",
    "7aacd71743fa6e5a1963fd9703be7e5e",

]

# (파일 저장시 인코딩 처리) CP949로 인코딩하면서 지원하지 않는 문자 삭제
def clean_keyword(keyword):
    cleaned_keyword = keyword.encode('cp949', errors='ignore').decode('cp949')
    return cleaned_keyword

# 로그 파일 작성하기
def log_to_file(message, filename = "log.txt"):
    with open(filename, 'a') as log_file:
        message = clean_keyword(message)
        log_file.write(message + '\n')
        
# 에러 로그 파일 작성하기        

# data_refer 분석해서 current_parsing에 넣음 -> buffers로 복사함
# 파싱 중간인 경우, buffers의 데이터를 가져감
def update_results():
    global buffers
    global current_pasring
        
    while(1):
        time.sleep(30)
        data_copy = data_refer.copy()
        
        # 순회하면서 데이터 추가하기
        for keywords, data in data_copy.items():
            for datum in data:
                # 블랙리스트
                existing_result = next((oldData for oldData in buffers if oldData['url'] == datum['url'] and 
                                        oldData['channel_id'] == datum['channel_id']), None)                    
                if existing_result:
                    if existing_result['title'] == datum['title']:
                        existing_result['last_updated'] = datum["time"]
                        continue
                    # 타이틀이 다른 경우
                    else:
                        existing_result['title'] = datum['title']
                        existing_result['time'] = datum['time']
                        existing_result['last_updated'] = datum['time']
                        continue            
                # 새로운 아이템인 경우
                else:
                    buffers.append({
                        'time': datum['time'],
                        'last_updated': datum['time'],
                        'title': datum['title'],
                        'channel_id': datum['channel_id'],
                        'channelImageurl': datum['channelImageurl'],
                        'channelName': datum['channelName'],
                        'url': datum['url'],
                        'platform': datum['platform']
                    })                           
        # 순회하면서 마지막 udpate가 15분 이상 지난 것들은 삭제하기             
        # 현재 시간
        now = datetime.now()

        # 2분 전 시간
        threshold_time = now - timedelta(minutes=2)

        # last_updated가 현재 시각보다 15분 이전인 아이템 삭제
        buffers = [item for item in buffers if datetime.strptime(item['last_updated'], '%Y-%m-%d %H:%M:%S') >= threshold_time]   
        
        # 복사하기
        current_pasring = buffers.copy()


def get_user_ip():
    return request.headers.get('X-Forwarded-For', request.remote_addr);


# 치지직에서 keyword 검색하는 함수
def fetch_live_chizz(keyword):    
    url = f"https://api.chzzk.naver.com/service/v1/search/lives?keyword={keyword}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    json_data = response.json()
    
    if json_data.get('code') == 200:
        data = json_data.get('content', {}).get('data', [])
        for item in data:
            _live = item.get('live', {})
            _tags = _live.get('tags', [])
            
            # 메이플스토리 하는 애들 (필터링 로직)
            liveCategoryValue = _live.get('liveCategoryValue', [])
            if '메이플스토리' in _tags or '메이플' in _tags:
                continue
            if '메이플스토리' in _tags or '메이플' in liveCategoryValue:
                continue
            
            name = _live.get('liveTitle')
            _channel_id = _live.get('channelId')
            
            if (_channel_id in chizzBannListID):
                continue            
            
            _channel_info = item.get("channel", [])
            channelImageurl = _channel_info.get("channelImageUrl")
            channelName = _channel_info.get("channelName")
            parsing_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if keyword not in data_refer:
                data_refer[keyword] = []
            
            data_refer[keyword].append({
                'time': parsing_time,
                'title': name,
                'channel_id': _channel_id,
                'channelImageurl': channelImageurl,
                'channelName': channelName,
                'url': f"https://chzzk.naver.com/live/{_channel_id}",
                'platform': 'chizz'
            })
        
        return True
    
    # 파싱에 실패한 경우    
    return False;

# 숲에서 keyword 검색하는 함수
def fetch_live_soop(keyword):
    keyword = keyword.encode('cp949', errors='ignore').decode('cp949')
    soop_keyword = urllib.parse.quote(keyword)
    url = f"https://sch.sooplive.co.kr/api.php?l=DF&m=liveSearch&c=UTF-8&w=webk&isMobile=0&onlyParent=1&szType=json&szOrder=score&szKeyword={soop_keyword}&nPageNo=1&nListCnt=12&tab=total&location=total_search&isHashSearch=0"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }    
    response = requests.get(url, headers=headers)
    json_data = response.json()
    
    # TODO 페이징있는 경우 페이징해서 원래 데이터 가져와야 함.
    if json_data.get('RESULT') == '1':
        data = json_data.get('REAL_BROAD', [])
        for item in data:
            name = item.get("broad_title")
            channelName = item.get("user_id")
            _channel_id = item.get("broad_no")
            _folder_name = channelName[:2]
            channelImageurl = f"https://stimg.sooplive.co.kr/LOGO/{_folder_name}/{channelName}/{channelName}.jpg"
            parsing_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _tags = item.get('category_tags', [])

            # TODO 필터링 적용 필요
            contains_keyword = any(keyword in name for keyword in keywords)            
            
            if '메이플스토리' in _tags or '메이플' in _tags or contains_keyword == False:      
                hash_tags = item.get('hash_tags', [])
                exists = any(element in hash_tags for element in keywords)
                # TODO 창팝 제외 키워드 로깅 필요
                if exists == False:       
                    name = clean_keyword(name)                    
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    log_to_file(f"[{current_time}] ${name}에는 창드컵 관련 키워드가 없습니다.", "keyword-log.txt")
                    continue
            
            if keyword not in data_refer:
                data_refer[keyword] = []

            data_refer[keyword].append({
                'time': parsing_time,
                'title': name,
                'channel_id': _channel_id,
                'channelImageurl': channelImageurl,
                'channelName': channelName,
                'url': f"https://play.sooplive.co.kr/{channelName}/{_channel_id}",
                'platform': 'soop'
            })
            
        return True;


    return False

def thread_attack_chizz():
    chizz_keyword_index = 0

    while(1):
        # TODO: 실패한 경우 실패한 시각, 키워드 추가하기
        if (fetch_live_chizz(keywords[chizz_keyword_index])):
            chizz_keyword_index += 1
            chizz_keyword_index %= len(keywords)
        else:
            print("치지직 데이터 가져오기 실패")
        time.sleep(10)    

def thread_attack_soop():
    soop_keyword_index = 0

    while(1):
        # TODO: 실패한 경우 실패한 시각, 키워드 추가하기
        if (fetch_live_soop(keywords[soop_keyword_index])):
            soop_keyword_index += 1
            soop_keyword_index %= len(keywords)
        else:
            print("숲 데이터 가져오기 실패")
        time.sleep(10)        

# 치지직 데이터 업데이트 하는 쓰레드
update_thread_chizz = threading.Thread(target=thread_attack_chizz)
update_thread_chizz.daemon = True
update_thread_chizz.start()

# 숲에서 데이터 업데이트 하는 쓰레드
update_thread_soop = threading.Thread(target=thread_attack_soop)
update_thread_soop.daemon = True
update_thread_soop.start()

update_thread_buffer = threading.Thread(target=update_results)
update_thread_buffer.daemon = True
update_thread_buffer.start()


# 메인 페이지
@app.route('/')
def index():
    return render_template('index2.html')

# 데이터를 보내주는 함수
@app.route('/api/live-streams')
def api_live_streams():
    return jsonify(current_pasring)


@app.route('/api/warning', methods=['POST'])
def warning():    
    # JSON 요청 데이터에서 channel_id 가져오기
    data = request.get_json()
    channel_id = data.get('channel_id')
    csv_file_path = "report.csv";

    # CSV 파일이 존재하는지 확인
    if os.path.exists(csv_file_path):
        # CSV 파일 읽기
        df = pd.read_csv(csv_file_path)

        # channel_id가 있는지 확인
        if channel_id in df['channel_id'].values:
            # channel_id의 count 증가
            df.loc[df['channel_id'] == channel_id, 'count'] += 1
        else:
            # channel_id 추가
            new_row = pd.DataFrame({'channel_id': [channel_id], 'count': [1]})
            df = pd.concat([df, new_row], ignore_index=True)
    else:
        # CSV 파일이 없으면 새로 생성
        df = pd.DataFrame({'channel_id': [channel_id], 'count': [1]})

    # CSV 파일에 저장
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_ip = get_user_ip()
    user_agent = request.headers.get('User-Agent')    
    log_message = f"[{current_time}] Reject - IP: {user_ip}, User-Agent: {user_agent}, ChannelID : {channel_id}";
    log_to_file(log_message, "warning.txt")
    
    return jsonify({"result":"success"});

if __name__ == '__main__':
    app.run(debug=False)
