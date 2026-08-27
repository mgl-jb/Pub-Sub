using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

[Collection(BrokerCollection.Name)]
public class DuplicateDetectionTests
{
    private readonly BrokerFixture _fixture;

    public DuplicateDetectionTests(BrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task A_repeated_message_id_is_suppressed_within_the_window()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            topicOptions: new TopicOptions
            {
                DuplicateDetectionEnabled = true,
                DuplicateDetectionWindow = TimeSpan.FromMinutes(10),
            });

        IReadOnlyList<PublishResult> first =
            await _fixture.PublishAsync(topic, TestHelpers.Message("once", messageId: "order-1"));

        IReadOnlyList<PublishResult> second =
            await _fixture.PublishAsync(topic, TestHelpers.Message("once", messageId: "order-1"));

        first[0].WasDuplicate.ShouldBeFalse();
        second[0].WasDuplicate.ShouldBeTrue();
        second[0].SequenceNumber.ShouldBe(
            first[0].SequenceNumber,
            "the caller is told which message theirs was a duplicate of");

        (await _fixture.ReceiveAsync(topic, subscription)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task The_same_message_id_is_accepted_again_after_the_window_lapses()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            topicOptions: new TopicOptions
            {
                DuplicateDetectionEnabled = true,
                DuplicateDetectionWindow = TimeSpan.FromMinutes(5),
            });

        await _fixture.PublishAsync(topic, TestHelpers.Message(messageId: "recurring"));

        _fixture.Clock.Advance(TimeSpan.FromMinutes(6));

        IReadOnlyList<PublishResult> later =
            await _fixture.PublishAsync(topic, TestHelpers.Message(messageId: "recurring"));

        later[0].WasDuplicate.ShouldBeFalse(
            "the window is bounded, so a later send with the same id is a new message");

        (await _fixture.ReceiveAsync(topic, subscription)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task Duplicate_detection_is_off_unless_the_topic_enables_it()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        await _fixture.PublishAsync(topic, TestHelpers.Message(messageId: "same"));
        IReadOnlyList<PublishResult> second =
            await _fixture.PublishAsync(topic, TestHelpers.Message(messageId: "same"));

        second[0].WasDuplicate.ShouldBeFalse();
        (await _fixture.ReceiveAsync(topic, subscription)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task Different_topics_do_not_share_a_detection_scope()
    {
        (string topicA, string subA) = await _fixture.CreateTopicWithSubscriptionAsync(
            topicOptions: new TopicOptions { DuplicateDetectionEnabled = true });

        (string topicB, string subB) = await _fixture.CreateTopicWithSubscriptionAsync(
            topicOptions: new TopicOptions { DuplicateDetectionEnabled = true });

        await _fixture.PublishAsync(topicA, TestHelpers.Message(messageId: "shared-id"));
        IReadOnlyList<PublishResult> onB =
            await _fixture.PublishAsync(topicB, TestHelpers.Message(messageId: "shared-id"));

        onB[0].WasDuplicate.ShouldBeFalse();

        (await _fixture.ReceiveAsync(topicA, subA)).Count.ShouldBe(1);
        (await _fixture.ReceiveAsync(topicB, subB)).Count.ShouldBe(1);
    }
}
