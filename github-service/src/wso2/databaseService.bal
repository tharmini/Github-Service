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
    while (repoIterator < response.length()) {
        boolean flag = true;
        string gitUuid = response[repoIterator].id.toString();
        string repoName = response[repoIterator].name.toString();
        string url = response[repoIterator].url.toString();
        int teamId = 1;
        json[] RepoUUIDsJson = retrieveAllRepos(orgId);
        int UUIDIterator = 0;
        while (UUIDIterator < RepoUUIDsJson.length()) {
            if(gitUuid == RepoUUIDsJson[UUIDIterator].GitUuid.toString()){
                flag = false;
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

