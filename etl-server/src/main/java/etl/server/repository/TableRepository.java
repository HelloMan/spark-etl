package etl.server.repository;

import etl.api.table.Table;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TableRepository extends JpaRepository<Table,Long> {

    @Query("select a from Table a  left join fetch a.fields f where a.name = :name")
    Table findByName(@Param("name") String name);

    @Query("select a.name from Table a")
    List<String> getTableNames();

}
