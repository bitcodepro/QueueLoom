using System.Collections.ObjectModel;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed class SortedMirrorTests
{
    private static readonly Comparison<string> ByLength = (left, right) => left.Length.CompareTo(right.Length);

    [Fact]
    public void WithoutASortTheViewFollowsTheSourceOrder()
    {
        ObservableCollection<string> source = ["ccc", "a"];
        var mirror = new SortedMirror<string>(source);

        source.Add("bb");
        source.Insert(0, "dddd");
        source.Remove("a");
        source.Move(0, 2);

        Assert.Equal(source, mirror.View);
    }

    [Fact]
    public void ASortOrdersWhatIsThereAndWhatArrives()
    {
        ObservableCollection<string> source = ["ccc", "a", "dddd"];
        var mirror = new SortedMirror<string>(source);

        mirror.SortBy(ByLength, descending: false);
        Assert.Equal(["a", "ccc", "dddd"], mirror.View);

        source.Add("bb");
        Assert.Equal(["a", "bb", "ccc", "dddd"], mirror.View);

        mirror.SortBy(ByLength, descending: true);
        Assert.Equal(["dddd", "ccc", "bb", "a"], mirror.View);

        source.Remove("ccc");
        Assert.Equal(["dddd", "bb", "a"], mirror.View);
    }

    [Fact]
    public void EqualItemsKeepTheOrderTheyArrivedIn()
    {
        ObservableCollection<string> source = ["x1", "a", "x2"];
        var mirror = new SortedMirror<string>(source);
        mirror.SortBy(ByLength, descending: false);

        source.Add("x3");

        Assert.Equal(["a", "x1", "x2", "x3"], mirror.View);
    }

    [Fact]
    public void ClearingTheSortRestoresTheSourceOrderAndAClearEmptiesTheView()
    {
        ObservableCollection<string> source = ["ccc", "a"];
        var mirror = new SortedMirror<string>(source);
        mirror.SortBy(ByLength, descending: false);

        mirror.SortBy(null, descending: false);
        Assert.Equal(["ccc", "a"], mirror.View);

        source.Clear();
        Assert.Empty(mirror.View);
    }

    [Fact]
    public void TheMessageListSortsByTheChosenColumnAndTogglesTheDirection()
    {
        var list = new MessageListSort();

        Assert.Equal(MessageSortColumn.None, list.Column);
        list.Toggle(MessageSortColumn.Enqueued);
        Assert.Equal((MessageSortColumn.Enqueued, false), (list.Column, list.Descending));
        list.Toggle(MessageSortColumn.Enqueued);
        Assert.Equal((MessageSortColumn.Enqueued, true), (list.Column, list.Descending));
        list.Toggle(MessageSortColumn.Enqueued);
        Assert.Equal(MessageSortColumn.None, list.Column);
        list.Toggle(MessageSortColumn.Source);
        Assert.Equal((MessageSortColumn.Source, false), (list.Column, list.Descending));
        Assert.Equal("SOURCE ▲", list.Header(MessageSortColumn.Source));
        Assert.Equal("ENQUEUED", list.Header(MessageSortColumn.Enqueued));
    }
}
