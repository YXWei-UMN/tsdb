import random

regions = ["cn-central-2", "cn-central-1", "cn-west-2", "cn-west-1", "cn-east-2", "cn-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "ap-south-1", "sa-east-1", "ap-south-2", "sa-east-2"]
os = ["centos7", "centos8", "RHEL8", "RHEL8.1", "RHEL8.2", "RHEL8.3", "RHEL8.4", "Fedora34", "Fedora33", "Fedora32", "Ubuntu18", "Ubuntu20"]
service_environment = ["production", "staging", "test"]
team = ["CHI", "SF", "LON", "NYC", "HK", "BJ", "SH"]

with open("/home/local/ASUAD/ywei103/test_data/devops100000.txt", "r") as f1, open("TS_80M", "w") as f2:
    id = 0
    line = f1.readline()
    while line:
        items = line.strip().split(",")
        for x in range(5):   # 80M / 0.9M / len(region) = 4.4444444
            items[1] = "host=" + str(id + x*900000)
            for i1 in range(len(regions)):
                items[2] = "region=" + regions[i1]
                items[3] = "datacenter=" + regions[i1] + str(chr(ord('a') + random.randint(0, 20)))
                for i in range(4, len(items)):
                    sub = items[i].split("=")
                    if sub[0] == "rack":
                        items[i] = "rack=" + str(random.randint(0, 200))
                    elif sub[0] == "os":
                        items[i] = "os=" + os[random.randint(0, len(os) - 1)]
                    elif sub[0] == "arch":
                        if sub[1] == "x86":
                            items[i] = "arch=x64"
                        else:
                            items[i] = "arch=x86"
                    elif sub[0] == "team":
                        items[i] = "team=" + team[random.randint(0, len(team) - 1)]
                    elif sub[0] == "service":
                        items[i] = "service=" + str(random.randint(0, 50))
                    elif sub[0] == "service_version":
                        if sub[1] == "0":
                            items[i] = "service_version=1"
                        else:
                            items[i] = "service_version=0"
                    elif sub[0] == "service_environment":
                        items[i] = "service_environment=" + service_environment[random.randint(0, len(service_environment) - 1)]
                items_write = items[1:]
                f2.write(",".join(items_write) + "\n")
        id=id+1
        line = f1.readline()