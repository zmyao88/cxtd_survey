from pymongo import MongoClient
import pymongo as pymongo
import simplejson as json                                                  
import datetime 
import os

try: 
    client = MongoClient('localhost', 27017)                                       
    db = client['xtd201510m']                                                    
    collection = db['users'] 
    print("connected successfully!")
except pymongo.errors.ConnectionFailure as e:
   print ("Could not connect to MongoDB: %s" % e)
client

### create calculation time period 
def get_time():
    campaign_start_date = datetime.date(2015, 10, 10)
    campaign_start_date.isocalendar()[1]
    
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) #- datetime.timedelta(days=3)
    #start = today - datetime.timedelta(today.weekday())
    #end = start + datetime.timedelta(days = 7)
    end = datetime.datetime.today()
    start = end - datetime.timedelta(days = 7)
    
    
    print (str(today.isocalendar()[1] - 
                campaign_start_date.isocalendar()[1]) +
           " weeks away from campaign start")
    
    return [start, end]

def answer_update(answer):
    fixed_ansers = ['虹口区', '杨浦区', '黄浦区', '静安区', '普陀区', '闸北区', '浦东新区', '徐汇区', '长宁区',
                '宝山区', '闵行区', '金山区', '松江区', '嘉定区', '奉贤区', '青浦区', '崇明县', '都不是', '华漕', 
                '徐泾', '龙柏', '北新泾', '天山', '七宝', '古北', '九亭', '长宁其他地区', '闵行其他地区', '青浦其他地区',
                '上海市其他地区', '是', '否', '瑞虹天地星星堂', '虹桥天地', '湖滨道购物中心', '步行', '自驾', '公交车', '地铁',
                '出租车', '都不是', '10分钟内', '10到20分钟', '20到30分钟', '30到45分钟', '45分钟以上', '宣传单', '户外广告', 
                '路过现场', '官方微信', '微信朋友圈', '大众点评', '非常满意', '满意', '一般', '不满意', '非常不满意', '餐饮选择不够丰富',
                '儿童设施不够方便', '楼层及商户导示不够清晰', '整体环境不够整洁', '缺少公共休息区域', '停车位不足或不够方便', '餐饮选择不够丰富',
                '儿童设施不够方便', '楼层及商户导视不够清晰', '整体环境不够整洁', '缺少公共休息区域', '停车位不足', '交通不够方便', '男宝宝', 
                '女宝宝', '还在孕期', '还在享受二人世界或单身贵族', '暂时还没有宝宝', '0-1岁', '1-3岁', '3-6岁', '6-10岁', '10岁以上',
                '与动漫相关的主题展览/活动', '艺术展', '话剧/儿童剧', '乐园游玩', '运动探险', 'DIY(做蛋糕/陶艺/玩具/沙画等)', '集市', 
                '角色扮演', '家庭聚会', '朋友聚会', '商务宴请', '孩子教育或娱乐', '看电影/K歌或其他娱乐', '看展览或演出', '购物']
    if answer not in fixed_ansers:
        answer = '其他'
    return(answer)

# getting actual start and end time of this report
start, end = get_time()
def getting_summary_reports(collection):
    member_summary = {}
    mobile_list = {}
    pickup_summary = {}

    for dat in collection.find({"$and": 
                                [ {"createdAt": {"$gte": start}},
                                {"createdAt": {"$lt": end}}
                                ]}):
        # Initialize summary breakdown by member_source
        member_source = dat['profile']['mall']
        if  member_source not in member_summary.keys():
            member_summary[member_source] = {'attend': 0,
                                            'complete': 0,
                                            'phone': 0}
        # Initialize phone number list
        if  member_source not in mobile_list.keys():
            mobile_list[member_source] = []
            
        # Initialize pickup breakdown
        if  member_source not in pickup_summary.keys():
            pickup_summary[member_source] = {}

        
        #attend
        member_summary[member_source]['attend'] = member_summary[member_source]['attend'] + 1
        #complete
        if 'phone' in dat['profile'].keys():
            member_summary[member_source]['phone'] = member_summary[member_source]['phone'] + 1
            mobile_list[member_source].append(dat['profile']['phone'])

        #phone num
        if 'answers' in dat['profile'].keys():
            if 'q12' in dat['profile']['answers'].keys():
                member_summary[member_source]['complete'] = member_summary[member_source]['complete'] + 1
        #pickup locs
        if 'from' in dat['profile']:
            loc = dat['profile']['from']
            if loc not in pickup_summary[member_source]:
                pickup_summary[member_source][loc] = 0
            pickup_summary[member_source][loc] = pickup_summary[member_source][loc] + 1
            
    return(member_summary, mobile_list, pickup_summary)
 
    
def getting_answer_summary_detail(collection):
    answer_summary = {}
    for dat in collection.find({"$and": 
                                [ {"createdAt": {"$gte": start}},
                                {"createdAt": {"$lt": end}}
                                ]}):
        
        # initialize with mall_name
        member_source = dat['profile']['mall']
        if  member_source not in answer_summary.keys():
            answer_summary[member_source] = dict()

        if 'answers' in dat['profile'].keys():
            questions = dat['profile']['answers'].keys()
            for quest in questions:
                if quest not in answer_summary[member_source].keys():
                    answer_summary[member_source][quest] = {'total': 0}
                raw_answers =  dat['profile']['answers'][quest]
                if isinstance(raw_answers, str):
                    ##replace user input with others
                    raw_answers = answer_update(raw_answers)
                    # here goes actual code
                    if raw_answers not in answer_summary[member_source][quest].keys():
                        answer_summary[member_source][quest][raw_answers] = 0
                    answer_summary[member_source][quest][raw_answers] = answer_summary[member_source][quest][raw_answers] + 1
                else:
                    for answer in raw_answers:
                        ##replace user input with others
                        answer = answer_update(answer)
                        # here goes actual code
                        if answer not in answer_summary[member_source][quest].keys():
                            answer_summary[member_source][quest][answer] = 0
                        answer_summary[member_source][quest][answer] = answer_summary[member_source][quest][answer] + 1
                answer_summary[member_source][quest]['total'] = answer_summary[member_source][quest]['total'] + 1
    return(answer_summary)



member_summary, mobile_list, pickup_summary = getting_summary_reports(collection)    
answer_summary = getting_answer_summary_detail(collection)

base_dir = os.path.dirname(os.path.abspath(__file__))
print(base_dir)

file_name_pt1 = str(datetime.date.today().year) + '_' + str(datetime.date.today().month) + '_' + str(datetime.date.today().day)
if not os.path.exists(file_name_pt1):
    os.makedirs(base_dir + '/' +  file_name_pt1)

