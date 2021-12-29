import configparser as cp


def get_config():
    props = cp.ConfigParser()
    props.read_file(open("src/main/resources/config.ini"))
    config_dict = {}
    for each_section in props.sections():
        for (config_key, config_val) in props.items(each_section):
            config_dict.update({config_key: config_val})
    return config_dict


def copy_local_to_hdfs():
    import subprocess
    configs = get_config()
    subprocess.check_call(f"hdfs dfs -copyFromLocal -f {configs['input.base.dir']} {configs['output.base.dir']}",
                          shell=True)


if __name__ == '__main__':
    get_config()
