package datasource;

/**
 * @ date: 2020/10/5 21:18
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */
public class Item {

    private String name;
    private Integer id;

    public Item() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }

}
