import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/io;

http:Client gitClientEP = new("https://api.github.com");

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