package etl.installer.core;

import lombok.Getter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@Getter
public enum Component {
    NAMENODE(true,false,false),
    SECONDARY_NAMENODE(true,false,false),
    HISTORY_SERVER(true,false,false),
    RESOURCE_MANAGER(true,false,false),
    HMASTER(true,false,false),
    HMASTER_BACKUP(true,false,false),
    ZOOKEEPER(true,false,false),

    DATANODE(false,true,false),
    NODE_MANAGER(false,true,false),
    REGION_SERVER(false,true,false),
    QUERY_SERVER(false,true,false),
    IGNIS(false,false,true);

    private boolean master;
    private boolean slave;
    private boolean client;

    Component(boolean master, boolean slave, boolean client) {
        this.master = master;
        this.slave = slave;
        this.client = client;
    }

    public static List<Component> getMasters() {
        List<Component> result = new LinkedList<>();
        result.addAll(Arrays.stream(Component.values()).filter(Component::isMaster).collect(Collectors.toList()));
        result.addAll(Stream.of( ZOOKEEPER, ZOOKEEPER).collect(Collectors.toList()));
        return result;
    }

    public static Set<Component> getSlaves() {
        return Arrays.stream(Component.values()).filter(Component::isSlave).collect(Collectors.toSet());
    }
    public static Set<Component> getClients() {
        return Arrays.stream(Component.values()).filter(Component::isClient).collect(Collectors.toSet());
    }
}
