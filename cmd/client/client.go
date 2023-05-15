package main

import (
	"bufio"
	"crfs/client"
	"crfs/common"
	"crfs/master"
	"crfs/rpc"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type CommandHandler struct {
	Handler func(c *client.Client, cmd string, args []string, usage string)
	Usage   string
}

var commandHandlers map[string]CommandHandler = map[string]CommandHandler{
	"touch":  {Handler: touch, Usage: "用法: touch FILE"},
	"mkdir":  {Handler: mkdir, Usage: "用法: mkdir DIRECTORY"},
	"rm":     {Handler: rm, Usage: "用法: rm FILE"},
	"rmdir":  {Handler: rmdir, Usage: "用法: rmdir DIRECTORY"},
	"rsf":    {Handler: rsf, Usage: "用法: rsf FILE"},
	"rsdir":  {Handler: rsdir, Usage: "用法: rsdir DIRECTORY"},
	"mv":     {Handler: move, Usage: "用法: mv SRC TARGET"},
	"ls":     {Handler: list, Usage: "用法: ls DIRECTORY"},
	"write":  {Handler: write, Usage: "用法: write [-c CONTENT] [-f LOCALFILE] FILE OFFSET, 不能同时指定-c和-f"},
	"append": {Handler: append, Usage: "用法: append [-c CONTENT] [-f LOCALFILE] FILE, 不能同时指定-c和-f"},
	"read":   {Handler: read, Usage: "用法: read [-c] [-f LOCALFILE] FILE OFFSET SIZE, 不能同时指定-c和-f"},
}

func main() {
	// 1.解析命令行参数
	masterAddress := flag.String("master", "", "master地址")
	var zookeeperAddresses common.StringSliceFlag
	flag.Var(&zookeeperAddresses, "zookeeper", "zookeeper集群地址")
	flag.Parse()

	if *masterAddress == "" || len(zookeeperAddresses) == 0 {
		fmt.Fprintln(os.Stderr, "参数错误")
		flag.Usage()
		return
	}

	// 2.创建master client
	rpcClients := rpc.MakeRealClients()
	masterClient, err := rpcClients.MakeClient(*masterAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	// 3.创建client
	c := client.MakeClient(masterClient, rpcClients)
	defer c.Close()

	// 4.处理client输入
	fmt.Println("连接成功")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">>> ")
		scanner.Scan()
		command := scanner.Text()

		parts := strings.Fields(command)
		if len(parts) == 0 {
			continue
		}

		if handleCommand(c, parts[0], parts[1:]) {
			break
		}
	}
	fmt.Println("退出")
}

func handleCommand(c *client.Client, cmd string, args []string) bool {
	switch {
	case cmd == "exit":
		return true
	case cmd == "help":
		for commandName, commandHandler := range commandHandlers {
			fmt.Println(commandName + commandHandler.Usage)
		}
	default:
		if commandHandler, ok := commandHandlers[cmd]; ok {
			commandHandler.Handler(c, cmd, args, commandHandler.Usage)
		} else {
			fmt.Fprintf(os.Stderr, "未知命令: %s", cmd)
		}
	}

	return false
}

func touch(c *client.Client, cmd string, args []string, usage string) {
	create(c, cmd, args, true, usage)
}

func mkdir(c *client.Client, cmd string, args []string, usage string) {
	create(c, cmd, args, false, usage)
}

func create(c *client.Client, cmd string, args []string, isFile bool, usage string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if err := c.Create(args[0], isFile); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	}
}

func rm(c *client.Client, cmd string, args []string, usage string) {
	delete(c, cmd, args, true, usage)
}

func rmdir(c *client.Client, cmd string, args []string, usage string) {
	delete(c, cmd, args, false, usage)
}

func delete(c *client.Client, cmd string, args []string, isFile bool, usage string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if err := c.Delete(args[0], isFile); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	}
}

func rsf(c *client.Client, cmd string, args []string, usage string) {
	restore(c, cmd, args, true, usage)
}

func rsdir(c *client.Client, cmd string, args []string, usage string) {
	restore(c, cmd, args, false, usage)
}

func restore(c *client.Client, cmd string, args []string, isFile bool, usage string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if err := c.Restore(args[0], isFile); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	}
}

func move(c *client.Client, cmd string, args []string, usage string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if err := c.Move(args[0], args[1]); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	}
}

func list(c *client.Client, cmd string, args []string, usage string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	fileInfos, err := c.List(args[0])
	if err != master.Success {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	timeFormat := "2006-01-02 15:04:05"
	maxSize := 0
	for _, fileInfo := range fileInfos {
		if fileInfo.Size > maxSize {
			maxSize = fileInfo.Size
		}
	}

	for _, fileInfo := range fileInfos {
		t := "-"
		if !fileInfo.IsFile {
			t = "d"
		}
		fmt.Printf("%s %-*d %s %s\n", t, len(strconv.Itoa(maxSize)), fileInfo.Size, fileInfo.LastUpdated.Format(timeFormat), fileInfo.Name)
	}
}

func write(c *client.Client, cmd string, args []string, usage string) {
	flagSet := flag.NewFlagSet(cmd, flag.ContinueOnError)
	console := flagSet.String("c", "", "从标准输入写入crfs")
	f := flagSet.String("f", "", "从本地读取数据写入crfs")
	err := flagSet.Parse(args)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if *console == "" && *f == "" {
		fmt.Fprintln(os.Stderr, "必须指定-c或-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if *console != "" && *f != "" {
		fmt.Fprintln(os.Stderr, "不能同时指定-c和-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if flagSet.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	path := flagSet.Arg(0)
	offset, err := strconv.Atoi(flagSet.Arg(1))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	var data []byte
	if *f != "" {
		d, err := os.ReadFile(*f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		data = d
	} else {
		data = []byte(*console)
	}

	if err := c.Write(path, offset, data); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("successfully write %s, size: %d\n", path, len(data))
	}
}

func append(c *client.Client, cmd string, args []string, usage string) {
	flagSet := flag.NewFlagSet(cmd, flag.ContinueOnError)
	console := flagSet.String("c", "", "从标准输入追加crfs")
	f := flagSet.String("f", "", "从本地读取数据追加crfs")
	err := flagSet.Parse(args)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if *console == "" && *f == "" {
		fmt.Fprintln(os.Stderr, "必须指定-c或-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if *console != "" && *f != "" {
		fmt.Fprintln(os.Stderr, "不能同时指定-c和-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if flagSet.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	path := flagSet.Arg(0)

	var data []byte
	if *f != "" {
		d, err := os.ReadFile(*f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		data = d
	} else {
		data = []byte(*console)
	}

	if offset, err := c.Append(path, data); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("successfully append %s, offset: %d, size: %d\n", path, offset, len(data))
	}
}

func read(c *client.Client, cmd string, args []string, usage string) {
	flagSet := flag.NewFlagSet(cmd, flag.ContinueOnError)
	console := flagSet.Bool("c", false, "读取crfs到标准输出")
	f := flagSet.String("f", "", "读取crfs到本地文件")
	err := flagSet.Parse(args)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if !*console && *f == "" {
		fmt.Fprintln(os.Stderr, "必须指定-c或-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if *console && *f != "" {
		fmt.Fprintln(os.Stderr, "不能同时指定-c和-f")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	if flagSet.NArg() < 3 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		fmt.Fprintln(os.Stderr, usage)
		return
	}

	path := flagSet.Arg(0)
	offset, err := strconv.Atoi(flagSet.Arg(1))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	size, err := strconv.Atoi(flagSet.Arg(2))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	if data, err := c.Read(path, offset, size); err != master.Success {
		fmt.Fprintln(os.Stderr, err)
	} else {
		if *f != "" {
			if err := os.WriteFile(*f, data, 0664); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		} else {
			fmt.Print(string(data))
		}
	}
}
