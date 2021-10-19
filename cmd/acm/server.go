package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Lunarhalos/go-acm/server"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// serverCommand represents the server command
var serverCommand = &cobra.Command{
	Use:   "server",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: serverRun,
}

func serverRun(cmd *cobra.Command, args []string) error {
	config := server.DefaultConfig()
	log.Info(viper.AllKeys())
	if err := viper.Unmarshal(config); err != nil {
		logrus.WithError(err).Fatal("config: Error unmarshalling config")
		return err
	}
	sv := server.New(config)
	if err := sv.Start(); err != nil {
		return err
	}
	exit := handleSignals(sv.Stop)
	if exit != 0 {
		return fmt.Errorf("exit status: %d", exit)
	}

	return nil
}

// handleSignals blocks until we get an exit-causing signal
func handleSignals(stop func() error) int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal
	var sig os.Signal
	select {
	case s := <-signalCh:
		sig = s
	}
	fmt.Printf("Caught signal: %v", sig)

	// Fail fast if not doing a graceful leave
	if sig != syscall.SIGTERM && sig != os.Interrupt {
		return 1
	}

	// Attempt a graceful leave
	log.Info("acm: Gracefully shutting down agent...")
	if err := stop(); err != nil {
		fmt.Printf("Error: %s", err)
		log.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	return 0
}
