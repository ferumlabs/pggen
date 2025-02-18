package gen

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/ferumlabs/pggen/gen/internal/config"
	"github.com/ferumlabs/pggen/gen/internal/log"
	"github.com/ferumlabs/pggen/gen/internal/meta"
	"github.com/ferumlabs/pggen/gen/internal/types"
	"github.com/ferumlabs/pggen/gen/internal/utils"
)

// `pggen.Config` contains a collection of configuration options for the
// the codegenerator
type Config struct {
	// The path to a configuration file in TOML format containing information
	// about the database objects that pggen should generate code for.
	ConfigFilePath string
	// The name of the file to which the output should be written.
	OutputFileName string
	// A list of postgres connection strings to be used to connect to the
	// database. They tried in order until one is found where `DB.Ping` works.
	ConnectionStrings []string
	// A list of var patterns which disable pggen when they match the environment.
	DisableVars []string
	// A list of var patterns which must match against the environment in order for
	// pggen to run.
	EnableVars []string
	// The verbosity level of the code generator. -1 means quiet mode,
	// 0 (the default) means normal mode, and 1 means verbose mode.
	Verbosity int
}

// An instantiation of a pggen codegenerator
type Generator struct {
	// The user supplied configuration for this run of the pggen
	// codegenerator.
	config Config
	// A logger to use to print output
	log *log.Logger
	// The name of the package that all generated code is a part of.
	// Inferred from `config.OutputFileName`.
	pkg string
	// The client we use to talk to postgres in order to get metadata
	// about the schema
	metaResolver *meta.Resolver
	// The packages which need to be imported into the emitted
	// file.
	imports map[string]bool
	// This generator should do nothing because a disable var matched
	disabledByDisableVar bool
	// This generated should do nothing becase an enable var failed to match
	disabledByEnableVar bool
	// Used to map postgres types to information we can use to codegen go types
	typeResolver *types.Resolver
}

func FromConfig(config Config) (*Generator, error) {
	logger := log.NewLogger(config.Verbosity)
	if anyVarPatternMatches(config.DisableVars) {
		return &Generator{log: logger, disabledByDisableVar: true}, nil
	}
	if !allVarPatternsMatch(config.EnableVars) {
		return &Generator{log: logger, disabledByEnableVar: true}, nil
	}

	// make sure the output file name is there and of the right form
	if len(config.OutputFileName) == 0 {
		config.OutputFileName = "./pg_generated.go"
	}
	if strings.HasSuffix(config.OutputFileName, ".go") &&
		!strings.HasSuffix(config.OutputFileName, ".gen.go") {
		config.OutputFileName = config.OutputFileName[:len(config.OutputFileName)-3] + ".gen.go"
	}

	// check that we have at least one connection string, and if not, fall back on DB_URL
	if len(config.ConnectionStrings) == 0 {
		config.ConnectionStrings = []string{os.Getenv("DB_URL")}
		if len(config.ConnectionStrings[0]) == 0 {
			return nil, fmt.Errorf("No connection string. Either pass '-c' or set DB_URL in the environment.")
		}
	}

	var err error
	var db *sql.DB
	for _, connStr := range config.ConnectionStrings {
		if len(connStr) == 0 {
			continue
		}

		if connStr[0] == '$' {
			connStr = os.Getenv(connStr[1:])
		}

		db, err = sql.Open("pgx", connStr)
		if err != nil {
			db = nil
			continue
		}

		err = db.Ping()
		if err == nil {
			break
		} else {
			db = nil
			continue
		}
	}
	if db == nil {
		return nil, fmt.Errorf(
			"unable to connect with any of the provided connection strings",
		)
	}

	pkg, err := utils.DirOf(config.OutputFileName)
	if err != nil {
		return nil, err
	}

	imports := initialImports()
	registerImport := func(importStr string) {
		imports[importStr] = true
	}
	typeResolver := types.NewResolver(db, registerImport)
	return &Generator{
		config:       config,
		log:          logger,
		metaResolver: meta.NewResolver(logger, db, typeResolver, registerImport),
		pkg:          pkg,
		imports:      imports,
		typeResolver: typeResolver,
	}, nil
}

func initialImports() map[string]bool {
	return map[string]bool{
		`"context"`: true,
	}
}

// Generate the code that this generator has been configured for
func (g *Generator) Gen() error {
	if g.disabledByDisableVar {
		g.log.Info("pggen: doing nothing because a disable var matched\n")
		return nil
	}

	if g.disabledByEnableVar {
		g.log.Info("pggen: doing nothing because an enable var failed to match\n")
		return nil
	}

	defer g.metaResolver.Close() // nolint: errcheck

	conf, err := g.setupGenEnv()
	if err != nil {
		return err
	}

	err = g.genPrelude()
	if err != nil {
		return err
	}

	//
	// Generate the code based on database objects
	//

	var body strings.Builder

	err = g.genPGClient(&body, conf)
	if err != nil {
		return err
	}

	// Tables must be generated first to ensure that the type for a table is generated
	// by genTables rather than synthesized from a query result.
	err = g.genTables(&body, conf.Tables)
	if err != nil {
		return err
	}

	err = g.genQueries(&body, conf.Queries, conf.RequireQueryComments)
	if err != nil {
		return err
	}

	err = g.genStmts(&body, conf.Stmts)
	if err != nil {
		return err
	}

	err = g.genInterfaces(&body, conf)
	if err != nil {
		return err
	}

	//
	// Write the generated code to the file
	//

	var out strings.Builder

	// generate imports
	_, err = out.WriteString("// Code generated by pggen DO NOT EDIT.\n")
	if err != nil {
		return err
	}
	_, err = out.WriteString(fmt.Sprintf(`
package %s

import (
`, g.pkg))
	if err != nil {
		return err
	}
	sortedPkgs := make([]string, 0, len(g.imports))
	for pkg := range g.imports {
		if len(pkg) > 0 {
			sortedPkgs = append(sortedPkgs, pkg)
		}
	}
	sort.Strings(sortedPkgs)
	for _, pkg := range sortedPkgs {
		_, err = out.WriteString(fmt.Sprintf("	%s\n", pkg))
		if err != nil {
			return err
		}
	}
	_, err = out.WriteString(")\n\n")
	if err != nil {
		return err
	}

	_, err = out.WriteString(body.String())
	if err != nil {
		return err
	}

	err = g.typeResolver.Gen(&out)
	if err != nil {
		return err
	}
	return utils.WriteGoFile(g.config.OutputFileName, []byte(out.String()))
}

func (g *Generator) setupGenEnv() (*config.DbConfig, error) {
	g.log.Infof("pggen: using config '%s'\n", g.config.ConfigFilePath)
	confData, err := ioutil.ReadFile(g.config.ConfigFilePath)
	if err != nil {
		return nil, err
	}

	// parse the config file
	var conf config.DbConfig
	tomlMd, err := toml.Decode(string(confData), &conf)
	if err != nil {
		return nil, fmt.Errorf("while parsing config file: %s", err.Error())
	}
	for _, unknownKey := range tomlMd.Undecoded() {
		fmt.Fprintf(
			os.Stderr,
			"WARN: unknown config file key: '%s'\n",
			unknownKey.String(),
		)
	}
	err = conf.Normalize()
	if err != nil {
		return nil, err
	}
	err = conf.Validate()
	if err != nil {
		return nil, err
	}

	err = g.typeResolver.Resolve(&conf)
	if err != nil {
		return nil, err
	}

	// Place metadata about all tables in a hashtable to later
	// access by the table and query generation phases.
	err = g.metaResolver.Resolve(&conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}
