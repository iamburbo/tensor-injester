package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/iamburbo/tensor-injester/partA"
	"github.com/iamburbo/tensor-injester/partB"
	"github.com/iamburbo/tensor-injester/partC"
	"github.com/iamburbo/tensor-injester/pipeline"
	"github.com/iamburbo/tensor-injester/util"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "injester",
	Short: "ingests Solana onchain sigantures from a given marketplace address",
	Run:   run,
}

func init() {
	rootCmd.PersistentFlags().StringP("mode", "m", util.MODE_STANDARD.String(), "mode: History or Standard")
	rootCmd.PersistentFlags().StringP("marketplace", "k", "TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN", "marketplace to fetch sigs for")
	rootCmd.PersistentFlags().IntP("siFetchSize", "f", 1000, "# of sigs to fetch at a time (max 1000)")
	rootCmd.PersistentFlags().IntP("sigWriteSize", "w", 50, "# of sigs to write at a time (max 50)")
	rootCmd.PersistentFlags().StringP("part", "p", "a", "which part of the pipeline to run (a, b, or c)")
	rootCmd.PersistentFlags().StringP("proxy", "x", "", "optional rpc proxy url")
}

func Execute() error {
	return rootCmd.Execute()
}

func run(cmd *cobra.Command, args []string) {
	// TODO: Flag this
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	err := godotenv.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("error loading .env file")
	}

	// Set up RPC client
	rpcClient := createRpcClient(cmd)

	// Parse flags
	mode := util.ParseMode(cmd.Flag("mode").Value.String())
	marketplace, err := solana.PublicKeyFromBase58(cmd.Flag("marketplace").Value.String())
	if err != nil {
		fmt.Println("Invalid marketplace address")
		os.Exit(1)
	}
	fetchSize, err := strconv.Atoi(cmd.Flag("siFetchSize").Value.String())
	if err != nil {
		fmt.Println("Invalid siFetchSize")
		os.Exit(1)
	}
	writeSize, err := strconv.Atoi(cmd.Flag("sigWriteSize").Value.String())
	if err != nil {
		fmt.Println("Invalid sigWriteSize")
		os.Exit(1)
	}
	part := strings.ToLower(cmd.Flag("part").Value.String())

	fmt.Println("Ingester reporting to duty 🫡")

	// Execute the injester pipeline
	pipelineCtx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	config := &pipeline.PipelineConfig{
		RpcClient:      rpcClient,
		Address:        marketplace,
		FetchLimit:     fetchSize,
		WriteSizeLimit: writeSize,
	}
	switch part {
	//
	//  Part A data pipelines
	//
	case "a":
		config.Filepath = "partA"
		if mode == util.MODE_HISTORY {
			pipeline, err := partA.NewHistoryPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		} else {
			pipeline, err := partA.NewStandardPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		}
	//
	//  Part B data pipelines
	//
	case "b":
		config.Filepath = "partB"
		if mode == util.MODE_HISTORY {
			pipeline, err := partB.NewHistoryPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		} else {
			pipeline, err := partB.NewStandardPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		}
	//
	//  Part C data pipelines
	//
	case "c":
		config.Filepath = "partC"
		if mode == util.MODE_HISTORY {
			pipeline, err := partC.NewHistoryPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		} else {
			pipeline, err := partC.NewStandardPipeline(pipelineCtx, config, wg)
			if err != nil {
				log.Printf("Error creating pipeline: %v", err)
				os.Exit(1)
			}

			go pipeline.Run(pipelineCtx)
		}
	default:
		fmt.Printf("Invalid part %s. Choose either A, B, or C\n", part)
		os.Exit(1)
	}

	// Block until SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	go func() {
		<-sigs
		cancel()
	}()

	// Wait for goroutines to finish and shutdown
	wg.Wait()
	fmt.Println("Job completed or SIGINT, Goodbye!")
	os.Exit(0)
}

// Creates a new RPC client from the RPC_PROVIDER env variable
// and handles proxying if the --proxy flag is set
func createRpcClient(cmd *cobra.Command) *rpc.Client {
	rpcUrl := os.Getenv("RPC_PROVIDER")
	if rpcUrl == "" {
		fmt.Println("Ooof missing RPC. Did you add one to .env?")
		os.Exit(1)
	}

	proxy := cmd.Flag("proxy").Value.String()
	if proxy != "" {
		proxyURL, err := url.Parse(proxy) // Change to your proxy address
		if err != nil {
			log.Fatal().Err(err).Msg("error parsing proxy url")
		}

		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.Proxy = http.ProxyURL(proxyURL)

		httpClient := &http.Client{
			Transport: customTransport,
			Timeout:   time.Second * 30, // Set your desired timeout
		}

		opts := &jsonrpc.RPCClientOpts{
			HTTPClient: httpClient,
		}
		rpcClient := jsonrpc.NewClientWithOpts(rpcUrl, opts)
		return rpc.NewWithCustomRPCClient(rpcClient)
	} else {
		return rpc.New(rpcUrl)
	}
}
