import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/io;
import ballerina/jsonutils;
//import ballerina/runtime;



http:Client gitClientEP = new("https://api.github.com");
http:Client gitClientIssue = new ("https://api.github.com/repos");

public function main() {
    int intervalInMillis = 360000;
    task:Scheduler timer = new({
         intervalInMillis: intervalInMillis,
         initialDelayInMillis: 0
    });

    service DBservice = service {
            resource function onTrigger() {
                updateReposTable();
                updateIssuesTable();
                getAllIssues();
                log:printInfo("Repo table is updated");
            }
    };

    var attachResult = timer.attach(DBservice);
    if (attachResult is error) {
        log:printError("Error attaching the service.");
        return;
    }

    var startResult = timer.start();
        if (startResult is error) {
            log:printError("Starting the task is failed.");
            return;
    }
}

function updateReposTable() {
    http:Request req = new;
    req.addHeader("Authorization", "token " + "ab8f076e93cd72ab0dfafb9096dbcedabca1e09f");
    int orgIterator = 0;
    json[] organizations = retrieveAllOrganizations();
    while (orgIterator < organizations.length()) {
        string reqURL = "/users/" + organizations[orgIterator].OrgName.toString() + "/repos";
        var response = gitClientEP->get(reqURL, message = req);
        if (response is http:Response) {
            string contentType = response.getHeader("Content-Type");
            int statusCode = response.statusCode;
            if (statusCode != 404)
            {
                var respJson = response.getJsonPayload();
                if( respJson is json) {
                    insertIntoReposTable(<json[]> respJson, <int> organizations[orgIterator].OrgId);
                    isRepoExist(<json[]> respJson, <int> organizations[orgIterator].OrgId);
                }
            }
        } else {
            log:printError("Error when calling the backend: " + response.reason());
        }
        orgIterator = orgIterator + 1;
    }
}

function updateIssuesTable() {
    http:Request req = new;
    req.addHeader("Authorization", "token " + "ab8f076e93cd72ab0dfafb9096dbcedabca1e09f");
    int orgIterator = 0;
    json[] organizations = retrieveAllOrganizations();
    json[] RepoUUIDsJson = retrieveAllRepos(<int> organizations[orgIterator].OrgId);
    //json[] lastUpdatedDate = retrieveLastUpdatedDate();
    while (orgIterator < organizations.length()) {
        int repoIterator = 0;
        while (repoIterator < RepoUUIDsJson.length()) {
            string reqURL = "/repos/" + organizations[orgIterator].OrgName.toString() + "/" + RepoUUIDsJson[repoIterator].RepoName.toString() + "/issues/events";
            io:println("url: ", reqURL);
            var response = gitClientEP->get(reqURL, message = req);
            if (response is http:Response) {
                string contentType = response.getHeader("Content-Type");
                io:println("Content-type: " ,contentType);
                int statusCode = response.statusCode;
                if (statusCode != 404)
                {
                    var respJson = response.getJsonPayload();
                    if( respJson is json) {
                        io:println(respJson.toString());
                    }
                }
            } else {
                log:printError("Error when calling the backend: " + response.reason());
            }
            repoIterator = repoIterator + 1;
        }
        orgIterator = orgIterator + 1;
    }
}





function getAllIssues() {

    http:Request req = new;
    req.addHeader("Authorization", "token " + "2af200b348ff44c2510560d0c342b92e14ebc603");
    int repoIterator = 0;
    json[] repositories = retrieveAllReposDetails();
    while (repoIterator < repositories.length()) {
        json[] RepoOrgsJson = [];
        int orgId = <int>repositories[repoIterator].OrgId;
        var RepoOrgs = GithubDb->select("SELECT ORG_NAME FROM ORGANIZATION WHERE ORG_ID=?", (), orgId);
        if (RepoOrgs is table<record {}>) {
            RepoOrgsJson = <json[]>jsonutils:fromTable(RepoOrgs);
            io:println(RepoOrgsJson.toString());
        } else {
            log:printError("Error occured while retrieving the oranization names from Database", err = RepoOrgs);
        }
        string orgName = RepoOrgsJson[0].ORG_NAME.toString();
        int repoId = <int>repositories[repoIterator].RepoId;
        string reqURL = "/" + orgName + "/" + repositories[repoIterator].RepoName.toString() + "/issues";
        var response = gitClientIssue->get(reqURL, message = req);
        if (response is http:Response) {
            string contentType = response.getHeader("Content-Type");
            int statusCode = response.statusCode;
            if (statusCode != 404)
            {
                var respJson = response.getJsonPayload();
                if (respJson is json) {

                    insertIntoIssueTable(<json[]>respJson, repoId);

                }
            }
        } else {
            log:printError("Error when calling the backend: ");
        }
        repoIterator = repoIterator + 1;
    }

}