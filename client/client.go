package client

type Client struct{}

func NewClient(masterAddr string) (c *Client, e error) {}

func (c *Client) CreateCollection(name string) (collection *Collection, e error) {}
