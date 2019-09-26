import ballerinax/java.jdbc;
//import ballerina/config;
import ballerina/log;
import ballerina/jsonutils;
//import ballerina/io;

jdbc:Client GithubDb = new({
        url: "jdbc:mysql://localhost:3306/WSO2_ORGANIZATION_DETAILS",
        username: "root",
        password: "root",
        poolOptions: { maximumPoolSize: 10 },
        dbOptions: { useSSL: false }
    });

type Organization record {
    int OrgId;
    string GitUuid;
    string OrgName;
};

type Repo record {
    int RepoId;
    string GitUuid;
    string RepoName;
    int OrgId;
    string url;
    int TeamId;
};




function retrieveAllOrganizations() returns json[] {
    var Organizations = GithubDb->select("SELECT * FROM ORGANIZATIONS", Organization);
    if (Organizations is table<Organization>) {
        json OrganizationJson = jsonutils:fromTable(Organizations);
            return <json[]>OrganizationJson;
    } else {
        log:printError("Error occured while retrieving the organization details from Database", err = Organizations);
    }
    return [];
}

function retrieveAllRepos(int OrgId) returns json[] {
    var Repos = GithubDb->select("SELECT * FROM REPOS WHERE ORG_ID=?", Repo , OrgId);
    if (Repos is table<Repo>) {
        json RepoJson = jsonutils:fromTable(Repos);
            return <json[]>RepoJson;
    } else {
        log:printError("Error occured while retrieving the repo details from Database", err = Repos);
    }
    return [];
}

function retrieveAllReposDetails() returns json[] {
    var Repositorys = GithubDb->select("SELECT * FROM REPOSITORY", Repo);
    if (Repositorys is table<Repo>) {
        json Repositorysjson = jsonutils:fromTable(Repositorys);
        return <json[]>Repositorysjson;
    } else {
        log:printError("Error occured while retrieving the product names from Database", err = Repositorys);
    }
    return [];
}


function retrieveLastUpdatedDate() returns json[] {
    var LastUpdatedDate = GithubDb->select("SELECT LAST_UPDATED_DATE FROM ISSUES", ());
    if (LastUpdatedDate is table< record {}>) {
        json LastUpdatedDateJson = jsonutils:fromTable(LastUpdatedDate);
            return <json[]>LastUpdatedDateJson;
    } else {
        log:printError("Error occured while retrieving the last updated date from Database", err = LastUpdatedDate);
    }
    return [];
}

function insertIntoReposTable(json[] response, int orgId) {
    int repoIterator = 0;
    json[] RepoUUIDsJson = [];
    while (repoIterator < response.length()) {
        boolean flag = true;
        string gitUuid = response[repoIterator].id.toString();
        string repoName = response[repoIterator].name.toString();
        string url = response[repoIterator].url.toString();
        int teamId = 1;
        json jsonReturnValue = {};
        var RepoUUIDs = GithubDb->select("SELECT GITHUB_UUID,REPO_ID,REPO_NAME,URL FROM REPOSITORY WHERE ORG_ID=?", (), orgId);
                if (RepoUUIDs is table<record {}>) {
                    RepoUUIDsJson = <json[]>jsonutils:fromTable(RepoUUIDs);
                } else {
                    log:printError("Error occured while retrieving the product names from Database", err = RepoUUIDs);
                }
         int UUIDIterator = 0;
         while (UUIDIterator < RepoUUIDsJson.length()) {
                    if (gitUuid == RepoUUIDsJson[UUIDIterator].GIT_UUID.toString()) {
                        flag = false;
                        if (repoName != RepoUUIDsJson[UUIDIterator].REPO_NAME.toString() || url != RepoUUIDsJson[UUIDIterator].URL.toString()){

                            var ret = GithubDb->update("UPDATE  REPOS SET REPO_NAME=?,URL=? WHERE GITHUB_UUID=?", repoName, url, gitUuid);

                        }

                    }
                    UUIDIterator = UUIDIterator + 1;
                }
        if(flag){
           var ret = GithubDb->update("INSERT INTO REPOS(GITHUB_UUID, REPO_NAME, ORG_ID, URL, TEAM_ID) Values (?,?,?,?,?)",
                                gitUuid, repoName, orgId, url, teamId);
        }
        repoIterator = repoIterator + 1;
    }
}


function insertIntoIssueTable(json[] response, int repoId) {
    int repoIterator = 0;
    string types;
    json[] RepoUUIDsJson = [];
    while (repoIterator < response.length()) {
        string createdTime = response[repoIterator].created_at.toString();
        string updatedTime = response[repoIterator].updated_at.toString();
        string closedTime = response[repoIterator].closed_at.toString();


        string type1 = response[repoIterator].pull_request.toString();

        if (type1 == "error {ballerina/lang.map}KeyNotFound message=Key 'pull_request' not found in JSON mapping") {
            types = "issues";
        }
        else {
            types = "PR";

        }
        int repo_Id = repoId;
        string createdby = response[repoIterator].user.login.toString();
        var ret = GithubDb->update("INSERT INTO ISSUES(REPO_ID,CREATED_DATE,LAST_UPADATED_DATE,CLOSED_DATE,
          CREATED_BY,ISSUE_TYPE) Values (?,?,?,?,?,?)", repo_Id, createdTime, updatedTime, closedTime, createdby, types);
        repoIterator = repoIterator + 1;
    }


}



function isRepoExist (json[] repoJson, int orgId) {
    int id =-1;
    int UUIDIterator = 0;
    json[] RepoUUIDsJson =retrieveAllRepos(orgId);
    while (UUIDIterator < RepoUUIDsJson.length()){
        int repoIterator = 0;
        boolean exists = true;
        while (repoIterator < repoJson.length()) {
            if(RepoUUIDsJson[UUIDIterator].GitUuid.toString() == repoJson[repoIterator].id.toString()) {
                    exists = false;
            }
            repoIterator = repoIterator + 1;
        }
        if(exists) {
            var ret = GithubDb->update("UPDATE REPOS SET ORG_ID=? WHERE GITHUB_UUID=?",id,RepoUUIDsJson[UUIDIterator].GITHUB_UUID.toString());
        }
        UUIDIterator = UUIDIterator + 1;
    }
}

