// The `cassandra` plugin for SHIELD implements backup + restore of one single
// keyspace on a Cassandra node.
//
// PLUGIN FEATURES
//
// This plugin implements functionality suitable for use with the following
// SHIELD Job components:
//
//   Target: yes
//   Store:  no
//
// PLUGIN CONFIGURATION
//
// The endpoint configuration passed to this plugin is used to identify which
// cassandra node to back up, and how to connect to it. Your endpoint JSON
// should look something like this:
//
//    {
//        "cassandra_host"         : "10.244.67.61",
//        "cassandra_port"         : "9042",             # native transport port
//        "cassandra_user"         : "username",
//        "cassandra_password"     : "password",
//        "cassandra_keyspace"     : "ksXXXX",           # Required
//        "cassandra_bindir"       : "/path/to/bindir",
//        "cassandra_datadir"      : "/path/to/datadir",
//        "cassandra_tar"          : "/path/to/tar"      # where is the tar utility?
//    }
//
// The plugin provides devault values for those configuration properties, as
// detailed below. When a default value suits your needs, you can just ommit
// it.
//
//    {
//        "cassandra_host"     : "127.0.0.1",
//        "cassandra_port"     : "9042",
//        "cassandra_user"     : "cassandra",
//        "cassandra_password" : "cassandra",
//        "cassandra_bindir"   : "/var/vcap/packages/cassandra/bin",
//        "cassandra_datadir"  : "/var/vcap/store/cassandra/data",
//        "cassandra_tar"      : "tar"
//    }
//
// BACKUP DETAILS
//
// Backup is limited to one single keyspace, and is made against one single
// node. To completely backup the given keyspace, the backup operation needs
// to be performed on all cluster nodes.
//
// RESTORE DETAILS
//
// Restore is limited to the single keyspace specified in the plugin config.
// When restoring, this keyspace config must be the same as the keyspace
// specified at backup time. Indeed, this plugin doesn't support restoring to
// a different keyspace.
//
// Restore should happen on the same node where the data has been backed up.
// To completely restore a keyspace, the restore operation should be performed
// on each node of the cluster, with the data that was backed up on that same
// node.
//
// DEPENDENCIES
//
// This plugin relies on the `nodetool` and `sstableloader` utilities. Please
// ensure that they are present on the cassandra node that will be backed up
// or restored.

package main

import (
	"os"
	"path/filepath"
	"strings"

	fmt "github.com/jhunt/go-ansi"

	"github.com/starkandwayne/shield/plugin"
)

const (
	DefaultHost     = "127.0.0.1"
	DefaultPort     = "9042"
	DefaultUser     = "cassandra"
	DefaultPassword = "cassandra"
	DefaultBinDir   = "/var/vcap/jobs/cassandra/bin"
	DefaultDataDir  = "/var/vcap/store/cassandra/data"
	DefaultTar      = "tar"

	VcapOwnership = "vcap:vcap"
	SnapshotName  = "shield-backup"
)

func main() {
	p := CassandraPlugin{
		Name:    "Cassandra Backup Plugin",
		Author:  "Orange",
		Version: "0.1.0",
		Features: plugin.PluginFeatures{
			Target: "yes",
			Store:  "no",
		},
		Example: `
{
  "cassandra_host"         : "127.0.0.1",      # optional
  "cassandra_port"         : "9042",           # optional
  "cassandra_user"         : "username",
  "cassandra_password"     : "password",
  "cassandra_keyspace"     : "db",
  "cassandra_bindir"       : "/path/to/bin",   # optional
  "cassandra_datadir"      : "/path/to/data",  # optional
  "cassandra_tar"          : "/bin/tar"        # Tar-compatible archival tool to use
}
`,
		Defaults: `
{
  "cassandra_host"     : "127.0.0.1",
  "cassandra_port"     : "9042",
  "cassandra_user"     : "cassandra",
  "cassandra_password" : "cassandra",
  "cassandra_bindir"   : "/var/vcap/packages/cassandra/bin",
  "cassandra_datadir"  : "/var/vcap/store/cassandra/data",
  "cassandra_tar"      : "tar"
}
`,
		Fields: []plugin.Field{
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_host",
				Type:     "string",
				Title:    "Cassandra Host",
				Help:     "The hostname or IP address of your Cassandra server.",
				Default:  "127.0.0.1",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_port",
				Type:     "port",
				Title:    "Cassandra Port",
				Help:     "The 'native transport' TCP port that Cassandra server is bound to, listening for incoming connections.",
				Default:  "9042",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_user",
				Type:     "string",
				Title:    "Cassandra Username",
				Help:     "Username to authenticate to Cassandra as.",
				Default:  "cassandra",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_password",
				Type:     "password",
				Title:    "Cassandra Password",
				Help:     "Password to authenticate to Cassandra as.",
				Default:  "cassandra",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_keyspace",
				Type:     "string",
				Title:    "Keyspace to Backup",
				Help:     "The name of the keyspace to backup.",
				Example:  "system",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_bindir",
				Type:     "abspath",
				Title:    "Path to Cassandra bin/ directory",
				Help:     "The absolute path to the bin/ directory that contains the `nodetool` and `sstableloader` commands.",
				Default:  "/var/vcap/packages/cassandra/bin",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_datadir",
				Type:     "abspath",
				Title:    "Path to Cassandra data/ directory",
				Help:     "The absolute path to the data/ directory that contains the Cassandra database files.",
				Default:  "/var/vcap/store/cassandra/data",
				Required: true,
			},
			plugin.Field{
				Mode:     "target",
				Name:     "cassandra_tar",
				Type:     "abspath",
				Title:    "Path to `tar` utility",
				Help:     "By default, the plugin will search the local `$PATH` to find the `tar` utility.",
				Default:  DefaultTar,
				Required: true,
			},
		},
	}

	plugin.Run(p)
}

type CassandraPlugin plugin.PluginInfo

type CassandraInfo struct {
	Host     string
	Port     string
	User     string
	Password string
	Keyspace string
	BinDir   string
	DataDir  string
	Tar      string
}

// This function should be used to return the plugin's PluginInfo, however you decide to implement it
func (p CassandraPlugin) Meta() plugin.PluginInfo {
	return plugin.PluginInfo(p)
}

// Called to validate endpoints from the command line
func (p CassandraPlugin) Validate(endpoint plugin.ShieldEndpoint) error {
	var (
		s    string
		err  error
		fail bool
	)

	s, err = endpoint.StringValueDefault("cassandra_host", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_host          %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_host}          using default node @C{%s}\n", DefaultHost)
	} else {
		fmt.Printf("@G{\u2713 cassandra_host}          @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_port", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_port          %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_port}          using default port @C{%s}\n", DefaultPort)
	} else {
		fmt.Printf("@G{\u2713 cassandra_port}          @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_user", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_user          %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_user}          using default user @C{%s}\n", DefaultUser)
	} else {
		fmt.Printf("@G{\u2713 cassandra_user}          @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_password", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_password      %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_password}      using default password @C{%s}\n", DefaultPassword)
	} else {
		fmt.Printf("@G{\u2713 cassandra_password}      @C{%s}\n", s)
	}

	s, err = endpoint.StringValue("cassandra_keyspace")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_keyspace      %s}\n", err)
		fail = true
	} else {
		fmt.Printf("@G{\u2713 cassandra_keyspace}      @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_bindir", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_bindir        %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_bindir}        using default @C{%s}\n", DefaultBinDir)
	} else {
		fmt.Printf("@G{\u2713 cassandra_bindir}        @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_datadir", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_datadir       %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_datadir}       using default @C{%s}\n", DefaultDataDir)
	} else {
		fmt.Printf("@G{\u2713 cassandra_datadir}       @C{%s}\n", s)
	}

	s, err = endpoint.StringValueDefault("cassandra_tar", "")
	if err != nil {
		fmt.Printf("@R{\u2717 cassandra_tar           %s}\n", err)
		fail = true
	} else if s == "" {
		fmt.Printf("@G{\u2713 cassandra_tar}           using default @C{%s}\n", DefaultTar)
	} else {
		fmt.Printf("@G{\u2713 cassandra_tar}           @C{%s}\n", s)
	}

	if fail {
		return fmt.Errorf("cassandra: invalid configuration")
	}
	return nil
}

// Backup one cassandra keyspace
func (p CassandraPlugin) Backup(endpoint plugin.ShieldEndpoint) error {
	cassandra, err := cassandraInfo(endpoint)
	if err != nil {
		return err
	}

	plugin.DEBUG("Cleaning any stale '%s' snapshot", SnapshotName)
	cmd := fmt.Sprintf("%s/nodetool clearsnapshot -t %s \"%s\"", cassandra.BinDir, SnapshotName, cassandra.Keyspace)
	plugin.DEBUG("Executing: `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDIN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Clean any stale snapshot}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Clean any stale snapshot}\n")

	defer func() {
		plugin.DEBUG("Clearing snapshot '%s'", SnapshotName)
		cmd := fmt.Sprintf("%s/nodetool clearsnapshot -t %s \"%s\"", cassandra.BinDir, SnapshotName, cassandra.Keyspace)
		plugin.DEBUG("Executing: `%s`", cmd)
		err := plugin.Exec(cmd, plugin.STDIN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Clean snapshot}\n")
			return
		}
		fmt.Fprintf(os.Stderr, "@G{\u2713 Clean snapshot}\n")
	}()

	plugin.DEBUG("Creating a new '%s' snapshot", SnapshotName)
	cmd = fmt.Sprintf("%s/nodetool snapshot -t %s \"%s\"", cassandra.BinDir, SnapshotName, cassandra.Keyspace)
	plugin.DEBUG("Executing: `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDIN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Create new snapshot}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Create new snapshot}\n")

	// Here we need to copy the snapshots/shield-backup directories into a
	// {keyspace}/{tablename} structure that we'll temporarily put in
	// /var/vcap/store/shield/cassandra. Then we can tar it all and stream
	// that to stdout.

	baseDir := "/var/vcap/store/shield/cassandra"
	plugin.DEBUG("Creating any missing directories for '%s', with 0755 permissions", baseDir)
	err = os.MkdirAll(baseDir, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Create base temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Create base temporary directory}\n")

	// Recursively remove /var/vcap/store/shield/cassandra/{keyspace}, if any
	tmpKeyspaceDir := filepath.Join(baseDir, cassandra.Keyspace)
	plugin.DEBUG("Removing any stale '%s' directory", tmpKeyspaceDir)
	cmd = fmt.Sprintf("rm -rf \"%s\"", tmpKeyspaceDir)
	plugin.DEBUG("Executing `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDOUT)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Clear base temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Clear base temporary directory}\n")

	defer func() {
		// Recursively remove /var/vcap/store/shield/cassandra/{keyspace}, if any
		plugin.DEBUG("Cleaning the '%s' directory up", tmpKeyspaceDir)
		cmd := fmt.Sprintf("rm -rf \"%s\"", tmpKeyspaceDir)
		plugin.DEBUG("Executing `%s`", cmd)
		err := plugin.Exec(cmd, plugin.STDOUT)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Clean base temporary directory}\n")
			return
		}
		fmt.Fprintf(os.Stderr, "@G{\u2713 Clean base temporary directory}\n")
	}()

	plugin.DEBUG("Creating directory '%s' with 0700 permissions", tmpKeyspaceDir)
	err = os.Mkdir(tmpKeyspaceDir, 0700)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Create temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Create temporary directory}\n")

	// Iterate through {dataDir}/{keyspace}/{tablename}/snapshots/shield-backup/*
	// and for all the immutable files we find here, we hard-link them
	// to /var/vcap/store/shield/cassandra/{keyspace}/{tablename}
	//
	// We chose to hard-link because copying those immutable files is
	// unnecessary anyway. It could lead to performance issues and would
	// consume twice the disk space it should.

	info, err := os.Lstat(cassandra.DataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files}\n")
		return err
	}
	if !info.IsDir() {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files}\n")
		return fmt.Errorf("cassandra DataDir is not a directory")
	}

	srcKeyspaceDir := filepath.Join(cassandra.DataDir, cassandra.Keyspace)
	dir, err := os.Open(srcKeyspaceDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files}\n")
		return err
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files}\n")
		return err
	}
	for _, tableDirInfo := range entries {
		if !tableDirInfo.IsDir() {
			continue
		}

		src_dir := filepath.Join(srcKeyspaceDir, tableDirInfo.Name(), "snapshots", SnapshotName)

		tableName := tableDirInfo.Name()
		if idx := strings.LastIndex(tableName, "-"); idx >= 0 {
			tableName = tableName[:idx]
		}

		dst_dir := filepath.Join(tmpKeyspaceDir, tableName)
		plugin.DEBUG("Creating destination directory '%s'", dst_dir)
		err = os.MkdirAll(dst_dir, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files}\n")
			return err
		}

		plugin.DEBUG("Hard-linking all '%s/*' files to '%s/'", src_dir, dst_dir)
		err = hardLinkAll(src_dir, dst_dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Recursive hard-link snapshot files in temp dir}\n")
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Recursive hard-link snapshot files in temp dir}\n")

	plugin.DEBUG("Setting ownership of all backup files to '%s'", VcapOwnership)
	cmd = fmt.Sprintf("chown -R vcap:vcap \"%s\"", tmpKeyspaceDir)
	plugin.DEBUG("Executing `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDOUT)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Set ownership of snapshot hard-links}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Set ownership of snapshot hard-links}\n")

	plugin.DEBUG("Streaming output tar file")
	cmd = fmt.Sprintf("%s -c -C /var/vcap/store/shield/cassandra -f - \"%s\"", cassandra.Tar, cassandra.Keyspace)
	plugin.DEBUG("Executing `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDOUT)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Stream tar of snapshots files}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Stream tar of snapshots files}\n")

	return nil
}

// Hard-link all files from 'src_dir' to the 'dst_dir'
func hardLinkAll(src_dir string, dst_dir string) (err error) {

	dir, err := os.Open(src_dir)
	if err != nil {
		return err
	}
	defer func() {
		dir.Close()
	}()

	entries, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	for _, tableDirInfo := range entries {
		if tableDirInfo.IsDir() {
			continue
		}
		src := filepath.Join(src_dir, tableDirInfo.Name())
		dst := filepath.Join(dst_dir, tableDirInfo.Name())

		err = os.Link(src, dst)
		if err != nil {
			return err
		}
	}
	return nil
}

// Restore one cassandra keyspace
func (p CassandraPlugin) Restore(endpoint plugin.ShieldEndpoint) error {
	cassandra, err := cassandraInfo(endpoint)
	if err != nil {
		return err
	}

	plugin.DEBUG("Creating directory '%s' with 0755 permissions", "/var/vcap/store/shield/cassandra")
	err = os.MkdirAll("/var/vcap/store/shield/cassandra", 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Create base temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Create base temporary directory}\n")

	keyspaceDirPath := filepath.Join("/var/vcap/store/shield/cassandra", cassandra.Keyspace)

	// Recursively remove /var/vcap/store/shield/cassandra/{cassandra.Keyspace}, if any
	cmd := fmt.Sprintf("rm -rf \"%s\"", keyspaceDirPath)
	plugin.DEBUG("Executing `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDOUT)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Clear base temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Clear base temporary directory}\n")

	defer func() {
		// plugin.DEBUG("Skipping recursive deletion of directory '%s'", keyspaceDirPath)

		// Recursively remove /var/vcap/store/shield/cassandra/{cassandra.Keyspace}, if any
		cmd := fmt.Sprintf("rm -rf \"%s\"", keyspaceDirPath)
		plugin.DEBUG("Executing `%s`", cmd)
		err := plugin.Exec(cmd, plugin.STDOUT)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Clean base temporary directory}\n")
			return
		}
		fmt.Fprintf(os.Stderr, "@G{\u2713 Clean base temporary directory}\n")
	}()

	cmd = fmt.Sprintf("%s -x -C /var/vcap/store/shield/cassandra -f -", cassandra.Tar)
	plugin.DEBUG("Executing `%s`", cmd)
	err = plugin.Exec(cmd, plugin.STDIN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Extract tar to temporary directory}\n")
		return err
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Extract tar to temporary directory}\n")

	// Iterate through all table directories /var/vcap/store/shield/cassandra/{cassandra.Keyspace}/{tablename}
	dir, err := os.Open(keyspaceDirPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Load all tables data}\n")
		return err
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@R{\u2717 Load all tables data}\n")
		return err
	}
	for _, tableDirInfo := range entries {
		if !tableDirInfo.IsDir() {
			continue
		}
		// Run sstableloader on each sub-directory found, assuming it is a table backup
		tableDirPath := filepath.Join(keyspaceDirPath, tableDirInfo.Name())
		cmd := fmt.Sprintf("%s/sstableloader -u \"%s\" -pw \"%s\" -d \"%s\" \"%s\"", cassandra.BinDir, cassandra.User, cassandra.Password, cassandra.Host, tableDirPath)
		plugin.DEBUG("Executing: `%s`", cmd)
		err = plugin.Exec(cmd, plugin.STDIN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@R{\u2717 Load all tables data}\n")
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "@G{\u2713 Load all tables data}\n")

	return nil
}

func (p CassandraPlugin) Store(endpoint plugin.ShieldEndpoint) (string, int64, error) {
	return "", 0, plugin.UNIMPLEMENTED
}

func (p CassandraPlugin) Retrieve(endpoint plugin.ShieldEndpoint, file string) error {
	return plugin.UNIMPLEMENTED
}

func (p CassandraPlugin) Purge(endpoint plugin.ShieldEndpoint, key string) error {
	return plugin.UNIMPLEMENTED
}

func cassandraInfo(endpoint plugin.ShieldEndpoint) (*CassandraInfo, error) {
	host, err := endpoint.StringValueDefault("cassandra_host", DefaultHost)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_HOST: '%s'", host)

	port, err := endpoint.StringValueDefault("cassandra_port", DefaultPort)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_PORT: '%s'", port)

	user, err := endpoint.StringValueDefault("cassandra_user", DefaultUser)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_USER: '%s'", user)

	password, err := endpoint.StringValueDefault("cassandra_password", DefaultPassword)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_PWD: '%s'", password)

	keyspace, err := endpoint.StringValue("cassandra_keyspace")
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_KEYSPACE: '%s'", keyspace)

	bindir, err := endpoint.StringValueDefault("cassandra_bindir", DefaultBinDir)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_BINDIR: '%s'", bindir)

	datadir, err := endpoint.StringValueDefault("cassandra_datadir", DefaultDataDir)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_DATADIR: '%s'", datadir)

	tar, err := endpoint.StringValueDefault("cassandra_tar", DefaultTar)
	if err != nil {
		return nil, err
	}
	plugin.DEBUG("CASSANDRA_TAR: '%s'", tar)

	return &CassandraInfo{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Keyspace: keyspace,
		BinDir:   bindir,
		DataDir:  datadir,
		Tar:      tar,
	}, nil
}
