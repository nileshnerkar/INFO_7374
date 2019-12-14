from AWS.AWSClient import AWSClient
import xmltodict
class MTurk():

    def __init__(self):
        self.__s3_client = AWSClient(aws_service='s3').client
        self.__mturk_client = AWSClient(
            aws_service='mturk', 
            endpoint_url='https://mturk-requester-sandbox.us-east-1.amazonaws.com').client

    def createHit(self):
        pass

    def __getReviewableHits(self):
        res = self.__mturk_client.list_reviewable_hits()['HITs']
        pass 

    def getHitResults(self, hit):
        m_assgn = self.__mturk_client.list_assignments_for_hit(HITId=hit)['Assignments']
        res={}
        cnt = 1
        if m_assgn:
            for assgn in m_assgn:
                xml = assgn['Answer']
                ans = xmltodict.parse(xml)['QuestionFormAnswers']['Answer'] 
                flag = False
                for i in ans:
                    if i['QuestionIdentifier'] == 'Single.s' and i['FreeText'] == 'true':
                        flag = True
                        continue

                    if flag and i['QuestionIdentifier'] == 'tag1':
                        res[f'{cnt}'] = i['FreeText']
                cnt+=1
            return res
        else:
            return "MTurk Task not assigned to Worker"