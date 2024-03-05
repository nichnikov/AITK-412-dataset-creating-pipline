import os
from pandas import DataFrame
import pandas as pd


def sim_dissim2dataset(sim_df: DataFrame, dissim_df: DataFrame, sim_share: float, wish_size: int):
    """
    Объединяет данные, в которых содержатся похожие ("парафразы") пары вопросов и непохожие пары ("не парафразы"),
    формирует один размеченный датасет

    Args:
        sim_df (DataFrame): датасет похожих пар вопросов и ответов
        dissim_df (DataFrame): датасет непохожих пар вопросов и ответов
        sim_share (float): доля "похожих" пар вопросов в итоговом датасете
        wish_size (int): желаемый размер итогового датасета
    """

    dissim_share = 1 - sim_share
    dataset_size = min([wish_size, min([dissim_df.shape[0]//dissim_share, sim_df.shape[0]//sim_share])])
    
    sim_size = int(dataset_size * sim_share)
    dissim_size = int(dataset_size * dissim_share)
    
    assert dissim_df.shape[0] >= dissim_size and sim_df.shape[0] >= sim_size
    
    sim_df = sim_df.sample(frac=1)
    dissim_df = dissim_df.sample(frac=1)
    
    datasets = {"similar": sim_df[:sim_size], "dissimilar": dissim_df[:dissim_size]}
    
    for item in datasets:
        datasets[item] = datasets[item][["LmQuery1", "Query1", "LmQuery2", "Query2"]]
        if item == "similar":
            datasets[item]["label"] = 1
        else:
            datasets[item]["label"] = 0    
        
    ds_df = pd.concat(datasets.values())    

    return ds_df.sample(frac=1)



def dataset_split(dataset_df: DataFrame, shares: dict):
    """
    Формирует из входящего датасета: 
    - тренировочную
    - валидационную
    - тестовую выборки

    Args:
        dataset_df (DataFrame): 
        shares (dict): {"train": float, "val": float, "test": float},  
                        shares["train"] + shares["val"] + shares["test"] must be equal 1.0
    """
        
    assert all(key in shares.keys() for key in ['train', 'val', 'test'])
    assert sum(shares.values()) == 1.0    
    
    lens = {}
    for sh_type in shares:
        lens[sh_type] = int(dataset_df.shape[0] * shares[sh_type])

    
    return {"train": dataset_df[: lens["train"]], 
            "val": dataset_df[lens["train"]:lens["train"] + lens["val"]], 
            "test": dataset_df[lens["train"] + lens["val"]:]}




if __name__ == "__main__":
    for sys_id in [1, 2, 3, 4, 8, 10, 11, 13, 14, 15, 16, 21, 22, 27, 28, 34, 37, 45, 47, 50, 51, 54, 55]:
        in_sim_fn = "".join(["similar_queries_sys_", str(sys_id), ".feather"])
        in_dissim_fn = "".join(["dissimilar_queries_sys_", str(sys_id), ".feather"])
        
        sim_df = pd.read_feather(os.path.join("datasets", in_sim_fn))
        dissim_df = pd.read_feather(os.path.join("datasets", in_dissim_fn))
        
        sim_share = 0.4
        dataset_df = sim_dissim2dataset(sim_df, dissim_df, sim_share, 1000000)
              
        shrs = {"train": 0.8,  "val": 0.1, "test": 0.1}
        splited_dfs_dct = dataset_split(dataset_df, shrs)

        for ds_type in splited_dfs_dct:
            out_fn = "_".join(["sys", str(sys_id), str(ds_type), "queries.feather"])
            splited_dfs_dct[ds_type].to_feather(os.path.join("splited_datasets", out_fn))
            