using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using GetAllChats.Models;
using System.Net;
using System.Text.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GetAllChats;

public class Function
{
    private readonly AmazonDynamoDBClient _client;
    private readonly DynamoDBContext _context;

    public Function()
    {
        _client = new AmazonDynamoDBClient();
        _context = new DynamoDBContext(_client);
    }

    public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
    {
        var userId = request.QueryStringParameters["userId"];
        request.QueryStringParameters.TryGetValue("pageSize", out var pageSizeString);
        int.TryParse(pageSizeString, out var pageSize);
        pageSize = pageSize == 0 ? 15 : pageSize;

        if (pageSize > 1000 || pageSize < 1)
        {
            return new APIGatewayProxyResponse()
            {
                StatusCode = (int)HttpStatusCode.OK,
                Headers = new Dictionary<string, string>
            {
                { "Content-Type", "application/json" },
                { "Access-Control-Allow-Origin", "*" }
            },
                Body = "Недопустимый размер страницы."
            };
        }

        request.QueryStringParameters.TryGetValue("lastId", out var lastId);

        List<Chat> chats = await GetAllChats(userId, pageSize, lastId);

        var result = new List<GetAllChatsResponseItem>(chats.Count);

        return new APIGatewayProxyResponse
        {
            StatusCode = (int)HttpStatusCode.OK,
            Headers = new Dictionary<string, string>
            {
                { "Content-Type", "application/json" },
                { "Access-Control-Allow-Origin", "*" }
            },

            Body = JsonSerializer.Serialize(result)
        };
    }

    private async Task<List<Chat>> GetAllChats(string userId, int pageSize, string lastId)
    {
        var user1 = new QueryOperationConfig()
        {
            IndexName = "user1-updatedDt-index",
            KeyExpression = new Expression()
            {
                ExpressionStatement = "user1 = :user and id > :lastId",
                ExpressionAttributeValues = new Dictionary<string, DynamoDBEntry>()
            {
                { ":user", userId },
                { ":lastId", lastId }
            }
            },
            Limit = pageSize
        };
        var user1Results = await _context.FromQueryAsync<Chat>(user1).GetRemainingAsync();

        var user2 = new QueryOperationConfig()
        {
            IndexName = "user2-updatedDt-index",
            KeyExpression = new Expression()
            {
                ExpressionStatement = "user2 = :user and id > :lastId",
                ExpressionAttributeValues = new Dictionary<string, DynamoDBEntry>()
            {
                { ":user", userId },
                { ":lastId", lastId }
            }
            },
            Limit = pageSize
        };
        var user2Results = await _context.FromQueryAsync<Chat>(user2).GetRemainingAsync();

        var allResults = user1Results.Concat(user2Results);

        return allResults.OrderBy(x => x.UpdateDt).ToList();
    }
}
