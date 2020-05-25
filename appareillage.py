from contextlib import ExitStack

from SortedInputFile import SortedInputFile


# utilities ########################


# utilities ########################


# TODO :  agreger le file_iterator avec la key_specification et ajouter d'autres infos ( Fichier logique, fichier physique ... )
# TODO : compter enregistrements lus
# TODO : appliquer ventiler les data sur des noms de zones si une description est fournie
def main():
    filename1 = "data/test.txt"
    key_spec1 = [(0, 5)]
    fields_specs1 = (5, 24, 1)
    fields_names1 = ('IDENT', 'LIBELLE', 'LETTRE')
    filename2 = "data/test2.txt"
    key_spec2 = [(5, 10)]
    fields_specs2 = (5, 5, 4)
    fields_names2 = ('CODE', 'IDENT-2', 'LABEL')

    done = False

    print("----------")
    f1 = SortedInputFile(filename1, key_spec1, logical_name=filename1, fields_desc=fields_specs1, fields_names=fields_names1)
    f2 = SortedInputFile(filename2, key_spec2, logical_name=filename2, fields_desc=fields_specs2, fields_names=fields_names2)
    input_files = [f1, f2]
    with ExitStack() as stack:
        files = [stack.enter_context(in_f) for in_f in input_files]
        chunks = []
        for f in input_files:
            chunks.append(f.process_iterator())
        appareiller(chunks)


class AppareillageContext():
    def __init__(self, all_fluxes, next_fluxes, current_fluxes):
        self.all_fluxes = all_fluxes
        self.next_fluxes = next_fluxes
        self.current_fluxes = current_fluxes

    @property
    def are_fluxes_exhausted(self) -> bool:
        # xx = [c for c in self.next_fluxes]
        # print( f"exhausted ? :{xx} -> {all(c is None for c in self.next_fluxes)}")
        return all(c is None for c in self.next_fluxes)

    @property
    def are_datas_empty(self) -> bool:
        return len(self.current_fluxes) == 0


def consomme(all_fluxes, next_fluxes):
    def recount(n, presents, all) -> int:
        """
        n-ieme -> n-ieme True dans presents
        m-ieme dans presents -> indice recherché dans all
        """
        trues = -1
        m: int = 0
        for m, active in enumerate(presents):
            if active:
                trues += 1
            if trues == n:
                return m
        # si incoherence -> exception
        raise Exception(f' recount(n:{n}, presents:{presents}, all:{all} ) failed !-> trues:{trues} , m:{m}')

    if next_fluxes is None:
        # ouverture de tous les flux
        next_fluxes = [next(_) for _ in all_fluxes]
        return AppareillageContext(all_fluxes, next_fluxes, [])

    chunks_keys = [getattr(_, 'key', None) for _ in next_fluxes]
    lowest_key: str = min((key for key in chunks_keys if key is not None))

    current_fluxes = [_ for _ in next_fluxes if _ is not None and _.key == lowest_key]
    presents_fluxes = [getattr(_, 'key', None) == lowest_key for _ in next_fluxes]
    try:

        n: int = 0
        for n, flux in enumerate(current_fluxes):
            idx_next = recount(n, presents_fluxes, current_fluxes)
            next_fluxes[idx_next] = next(all_fluxes[idx_next], None)

#    except ValueError:
#        print(f"ValueError all none in chunks_keys {chunks_keys}")
#        current_fluxes = next_fluxes

    except  StopIteration:
        print(f"StopIteration {n}")
    finally:
        return AppareillageContext(all_fluxes, next_fluxes, current_fluxes)


def appareiller(fluxes, stop_at=20):
    """
    appareiller une liste d'iterateurs sur des fichiers
    """
    # premiere lecture sur tous les fichiers
    # FIXME : agreger le retour de consomme pour obtenir une structure regroupant:
    # FIXME : (fluxes, next_fluxes, datas) + plus un peu de contexte ( compteurs sur fichiers + indicateurs présents/absents ... )
    context = consomme(fluxes, None)
    fluxes = context.all_fluxes
    next_fluxes = context.next_fluxes
    datas = context.current_fluxes
    # on boucle tant que les flux ne sont pas epuisés et que l'on a des datas
    # while not (all(c is None for c in next_fluxes) and len(datas)==0) and stop_at > 0:
    while not (context.are_fluxes_exhausted and context.are_datas_empty) and stop_at > 0:
        # print(f"exhausted : {context.are_fluxes_exhausted} and datas_empty {context.are_datas_empty}")
        stop_at -= 1
        
        for data in datas:
            print(f'process(data={data}) ...')
            
            
            
        if not context.are_fluxes_exhausted:
            context = consomme(fluxes, next_fluxes)
            fluxes = context.all_fluxes
            next_fluxes = context.next_fluxes
            datas = context.current_fluxes


if __name__ == '__main__':
    main()
    print("<<<terminé>>>")
